/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.griffin;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.mp.FanOut;
import io.questdb.mp.MPSequence;
import io.questdb.mp.SCSequence;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.str.LPSZ;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TableWriterAsyncCmdTest extends AbstractGriffinTest {

    private final SCSequence commandReplySequence = new SCSequence();
    private final int engineCmdQueue = engine.getConfiguration().getWriterCommandQueueCapacity();
    private final int engineEventQueue = engine.getConfiguration().getWriterCommandQueueCapacity();

    @Test
    public void testAsyncAlterCommandsExceedEngineEventQueue() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table product (timestamp timestamp)", sqlExecutionContext);

            // Block event queue with stale sequence
            SCSequence staleSequence = new SCSequence();
            setUpEngineAsyncWriterEventWait(engine, staleSequence);

            SCSequence tempSequence = new SCSequence();
            try (TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "product", "test lock")) {
                for (int i = 0; i < engineEventQueue; i++) {
                    CompiledQuery cc = compiler.compile("ALTER TABLE product add column column" + i + " int", sqlExecutionContext);
                    executeNoWait(tempSequence, cc);
                    engine.tick();
                    writer.tick();
                }

                // Add column when event queue is stalled
                CompiledQuery cc = compiler.compile("ALTER TABLE product add column column5 int", sqlExecutionContext);
                try (QueryFuture qf = cc.execute(tempSequence)) {
                    qf.await(0);
                    engine.tick();
                    writer.tick();
                    Assert.assertFalse(qf.await(500_000));
                }

                // Remove sequence
                stopEngineAsyncWriterEventWait(engine, staleSequence);

                // Re-execute last query
                try (QueryFuture qf = cc.execute(tempSequence)) {
                    qf.await(0);
                    engine.tick();
                    writer.tick();

                    try {
                        qf.await();
                        Assert.fail();
                    } catch (SqlException exception) {
                        TestUtils.assertContains(exception.getFlyweightMessage(), "Duplicate column name: column5");
                    }
                }
            }
        });
    }

    @Test
    public void testAsyncAlterCommandsExceedsEngineCmdQueue() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table product (timestamp timestamp)", sqlExecutionContext);
            SCSequence tempSequence = new SCSequence();

            // Block table
            try (TableWriter ignored = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "product", "test lock")) {

                for (int i = 0; i < engineCmdQueue; i++) {
                    CompiledQuery cc = compiler.compile("ALTER TABLE product add column column" + i + " int", sqlExecutionContext);
                    executeNoWait(tempSequence, cc);
                    engine.tick();
                }

                try {
                    CompiledQuery cc = compiler.compile("ALTER TABLE product add column column5 int", sqlExecutionContext);
                    try (QueryFuture ignored1 = cc.execute(tempSequence)) {
                        Assert.fail();
                    }
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "Could not publish writer ALTER TABLE task [table=product]");
                }
            } // Unblock table

            CompiledQuery cc = compiler.compile("ALTER TABLE product add column column5 int", sqlExecutionContext);
            try (QueryFuture cq = cc.execute(tempSequence)) {
                // Should execute in sync since writer is unlocked
                Assert.assertTrue(cq.isDone());
            }
        });
    }

    @Test
    public void testAsyncAlterCommandsFailsToDropPartition() throws Exception {
        assertMemoryLeak(() -> {
            ff = new FilesFacadeImpl() {
                int attempt = 0;

                @Override
                public boolean rename(LPSZ from, LPSZ to) {
                    if (Chars.endsWith(from, "_meta") && attempt++ < configuration.getFileOperationRetryCount()) {
                        return false;
                    }
                    return super.rename(from, to);
                }
            };
            compile("create table product as (select x, x as to_remove from long_sequence(100))", sqlExecutionContext);

            QueryFuture cf;
            // Block table
            try (TableWriter ignored = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "product", "test lock")) {
                CompiledQuery cc = compiler.compile("ALTER TABLE product drop column to_remove", sqlExecutionContext);
                cf = cc.execute(commandReplySequence);
                engine.tick();
            } // Unblock table

            try {
                cf.await();
            } catch (SqlException exception) {
                TestUtils.assertContains(exception.getFlyweightMessage(), "cannot drop column. Try again later");
            } finally {
                cf.close();
            }
            compile("ALTER TABLE product drop column to_remove", sqlExecutionContext);
        });
    }

    @Test
    public void testAsyncAlterCommandsFailsToRemoveColumn() throws Exception {
        assertMemoryLeak(() -> {
            ff = new FilesFacadeImpl() {
                int attempt = 0;

                @Override
                public boolean rename(LPSZ from, LPSZ to) {
                    if (Chars.endsWith(from, "_meta") && attempt++ < configuration.getFileOperationRetryCount()) {
                        return false;
                    }
                    return super.rename(from, to);
                }
            };
            compile("create table product as (select x, x as to_remove from long_sequence(100))", sqlExecutionContext);

            // Block table
            try (TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "product", "test lock")) {
                CompiledQuery cc = compiler.compile("ALTER TABLE product drop column to_remove", sqlExecutionContext);
                try (QueryFuture queryFuture = cc.execute(new SCSequence())) {
                    engine.tick();
                    writer.tick(true);

                    try {
                        queryFuture.await(1_000_000);
                        Assert.fail();
                    } catch (SqlException exception) {
                        Assert.assertNotNull(exception);
                        TestUtils.assertContains(exception.getFlyweightMessage(), "cannot drop column. Try again later");
                    }
                }

            } // Unblock table
            Assert.assertTrue(compiler.compile("ALTER TABLE product drop column to_remove", sqlExecutionContext).execute(null).isDone());
        });
    }

    @Test
    public void testAsyncAlterDoesNotCommitUncommittedRowsOnWriterClose() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table product (timestamp timestamp, name symbol nocache) timestamp(timestamp)", sqlExecutionContext);
            QueryFuture commandFuture = null;
            try {
                try (TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "product", "test lock")) {
                    CompiledQuery cc = compiler.compile("alter table product alter column name cache", sqlExecutionContext);
                    commandFuture = cc.execute(commandReplySequence);

                    // Add 1 row
                    TableWriter.Row row = writer.newRow(0);
                    row.putSym(1, "s");
                    row.append();
                    // No commit
                }

                commandFuture.await();
                engine.releaseAllReaders();

                try (TableReader rdr = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), "product")) {
                    Assert.assertEquals(0, rdr.size());
                }
            } finally {
                if (commandFuture != null) {
                    commandFuture.close();
                }
            }
        });
    }

    @Test
    public void testAsyncAlterNonExistingTable() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table product (timestamp timestamp, name symbol nocache)", sqlExecutionContext);
            QueryFuture cf = null;
            try {
                try (TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "product", "test lock")) {
                    AlterStatementBuilder creepyAlter = new AlterStatementBuilder();
                    creepyAlter.ofDropColumn(1, "product", writer.getMetadata().getId());
                    creepyAlter.ofDropColumn("timestamp");
                    CompiledQueryImpl cc = new CompiledQueryImpl(engine).withDefaultContext(sqlExecutionContext);
                    cc.ofAlter(creepyAlter.build());
                    cf = cc.execute(commandReplySequence);
                }
                compile("drop table product", sqlExecutionContext);
                engine.tick();

                // ALTER TABLE should be executed successfully on writer.close() before engine.tick()
                cf.await();
            } finally {
                if (cf != null) {
                    cf.close();
                }
            }
        });
    }

    @Test
    public void testAsyncAlterSymbolCache() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table product (timestamp timestamp, name symbol nocache)", sqlExecutionContext);
            QueryFuture commandFuture = null;
            try {
                try (TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "product", "test lock")) {
                    CompiledQuery cc = compiler.compile("alter table product alter column name cache", sqlExecutionContext);
                    commandFuture = cc.execute(commandReplySequence);
                    writer.tick();
                    engine.tick();
                }

                commandFuture.await();
                engine.releaseAllReaders();

                try (TableReader rdr = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), "product")) {
                    int colIndex = rdr.getMetadata().getColumnIndex("name");
                    Assert.assertTrue(rdr.getSymbolMapReader(colIndex).isCached());
                }
            } finally {
                if (commandFuture != null) {
                    commandFuture.close();
                }
            }
        });
    }

    @Test
    public void testAsyncRenameMultipleColumns() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table product (timestamp timestamp, name symbol nocache)", sqlExecutionContext);
            QueryFuture commandFuture = null;
            try {

                try (TableWriter ignored = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "product", "test lock")) {
                    // Add invalid command to engine queue
                    MPSequence commandPugSeq = messageBus.getTableWriterCommandPubSeq();
                    long pubCursor = commandPugSeq.next();
                    Assert.assertTrue(pubCursor > -1);
                    messageBus.getTableWriterCommandQueue().get(pubCursor).setTableId(ignored.getMetadata().getId());
                    commandPugSeq.done(pubCursor);

                    CompiledQuery cc = compiler.compile("alter table product rename column name to name1, timestamp to timestamp1", sqlExecutionContext);
                    commandFuture = cc.execute(commandReplySequence);
                }
                engine.tick();

                commandFuture.await();

                engine.releaseAllReaders();
                try (TableReader rdr = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), "product")) {
                    Assert.assertEquals(0, rdr.getMetadata().getColumnIndex("timestamp1"));
                    Assert.assertEquals(1, rdr.getMetadata().getColumnIndex("name1"));
                }
            } finally {
                if (commandFuture != null) {
                    commandFuture.close();
                }
            }
        });
    }

    @Test
    public void testCommandQueueReused() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table product (timestamp timestamp)", sqlExecutionContext);

            // Block event queue with stale sequence
            try (TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "product", "test lock")) {
                for (int i = 0; i < 2 * engineEventQueue; i++) {
                    CompiledQuery cc = compiler.compile("ALTER TABLE product add column column" + i + " int", sqlExecutionContext);
                    try (QueryFuture cf = cc.execute(commandReplySequence)) {
                        engine.tick();
                        writer.tick();
                        cf.await();
                    }
                }

                Assert.assertEquals(2L * engineEventQueue + 1, writer.getMetadata().getColumnCount());
            }
        });
    }

    @Test
    public void testInvalidAlterDropPartitionStatementQueued() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table product (timestamp timestamp, name symbol nocache)", sqlExecutionContext);

            try (TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "product", "test lock")) {
                AlterStatementBuilder creepyAlter = new AlterStatementBuilder();
                creepyAlter.ofDropPartition(0, "product", writer.getMetadata().getId()).ofPartition(0);
                CompiledQueryImpl cc = new CompiledQueryImpl(engine).withDefaultContext(sqlExecutionContext);
                cc.ofAlter(creepyAlter.build());
                try (QueryFuture cf = cc.execute(commandReplySequence)) {
                    engine.tick();
                    writer.tick();

                    try {
                        cf.await();
                        Assert.fail();
                    } catch (SqlException exception) {
                        TestUtils.assertContains(exception.getFlyweightMessage(), "could not remove partition 'default'");
                    }
                }
            }
        });
    }

    @Test
    public void testInvalidAlterStatementQueued() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table product (timestamp timestamp, name symbol nocache)", sqlExecutionContext);

            try (TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "product", "test lock")) {

                AlterStatementBuilder creepyAlter = new AlterStatementBuilder();
                creepyAlter.ofDropColumn(1, "product", writer.getMetadata().getId());
                creepyAlter.ofDropColumn("timestamp").ofDropColumn("timestamp");
                CompiledQueryImpl cc = new CompiledQueryImpl(engine).withDefaultContext(sqlExecutionContext);
                cc.ofAlter(creepyAlter.build());

                try (QueryFuture commandFuture = cc.execute(commandReplySequence)) {
                    engine.tick();
                    writer.tick(true);
                    try {
                        commandFuture.await();
                        Assert.fail();
                    } catch (SqlException exception) {
                        TestUtils.assertContains(exception.getFlyweightMessage(), "Invalid column: timestamp");
                    }
                }
            }
        });
    }

    /***
     *
     * @param engine Cairo Engine to consume events from
     * @param sequence sequence to subscribe ot the events
     */
    private static void setUpEngineAsyncWriterEventWait(CairoEngine engine, SCSequence sequence) {
        final FanOut writerEventFanOut = engine.getMessageBus().getTableWriterEventFanOut();
        writerEventFanOut.and(sequence);
    }

    /***
     * Cleans up execution wait sequence to listen to the Engine async writer events
     * @param engine Cairo Engine subscribed to
     * @param sequence to unsubscribe from Writer Events
     */
    private static void stopEngineAsyncWriterEventWait(CairoEngine engine, SCSequence sequence) {
        engine.getMessageBus().getTableWriterEventFanOut().remove(sequence);
        sequence.clear();
    }

    private void executeNoWait(SCSequence tempSequence, CompiledQuery cc) throws SqlException {
        try (QueryFuture cq = cc.execute(tempSequence)) {
            cq.await(0);
        }
    }

}