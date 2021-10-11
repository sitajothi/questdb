/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cutlass.line.tcp;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cutlass.line.CharSequenceCache;
import io.questdb.cutlass.line.LineProtoTimestampAdapter;
import io.questdb.cutlass.line.udp.CairoLineProtoParserListener;
import io.questdb.std.*;
import io.questdb.std.str.FloatingDirectCharSink;

public class CairoQueueLineProtoParserListener extends CairoLineProtoParserListener {
    private final LineTcpMeasurementScheduler.NetworkIOJob netIoJob;
    private final LineTcpMeasurementScheduler scheduler;
    private LineTcpMeasurementScheduler.TableUpdateDetails tableUpdateDetails;

    public CairoQueueLineProtoParserListener(CairoEngine engine, CairoSecurityContext cairoSecurityContext, LineProtoTimestampAdapter timestampAdapter, int defaultPartitionBy, LineTcpMeasurementScheduler.NetworkIOJob netIoJob,  LineTcpMeasurementScheduler scheduler) {
        super(engine, cairoSecurityContext, timestampAdapter, defaultPartitionBy);
        this.netIoJob = netIoJob;
        this.scheduler = scheduler;
        MY_NEW_LINE_END = this::createTableAppendRow;
    }

    private void createTableAppendRow(CharSequenceCache cache) {
        tableUpdateDetails = scheduler.startNewMeasurementEvent(netIoJob, cache.get(tableName));

    }

    void createMeasurementEvent(
            LineTcpMeasurementScheduler.TableUpdateDetails tableUpdateDetails,
            LineTcpMeasurementScheduler.TableUpdateDetails.ThreadLocalDetails localDetails,
            FloatingDirectCharSink floatingCharSink
    ) {
        threadId = INCOMPLETE_EVENT_ID;
        this.tableUpdateDetails = tableUpdateDetails;
        long timestamp = protoParser.getTimestamp();
        if (timestamp != NewLineProtoParser.NULL_TIMESTAMP) {
            timestamp = timestampAdapter.getMicros(timestamp);
        }
        long bufPos = bufLo;
        long bufMax = bufLo + bufSize;
        Unsafe.getUnsafe().putLong(bufPos, timestamp);
        bufPos += Long.BYTES;
        int nEntities = protoParser.getnEntities();
        Unsafe.getUnsafe().putInt(bufPos, nEntities);
        bufPos += Integer.BYTES;
        for (int nEntity = 0; nEntity < nEntities; nEntity++) {
            if (bufPos + Long.BYTES < bufMax) {
                ProtoEntity entity = protoParser.getEntity(nEntity);
                int colIndex = localDetails.getColumnIndex(entity.getName());
                if (colIndex < 0) {
                    int colNameLen = entity.getName().length();
                    Unsafe.getUnsafe().putInt(bufPos, -1 * colNameLen);
                    bufPos += Integer.BYTES;
                    if (bufPos + colNameLen < bufMax) {
                        Vect.memcpy(entity.getName().getLo(), bufPos, colNameLen);
                    } else {
                        throw CairoException.instance(0).put("queue buffer overflow");
                    }
                    bufPos += colNameLen;
                } else {
                    Unsafe.getUnsafe().putInt(bufPos, colIndex);
                    bufPos += Integer.BYTES;
                }
                switch (entity.getType()) {
                    case NewLineProtoParser.ENTITY_TYPE_TAG: {
                        long tmpBufPos = bufPos;
                        int l = entity.getValue().length();
                        bufPos += Integer.BYTES + Byte.BYTES;
                        long hi = bufPos + 2L * l;
                        if (hi < bufMax) {
                            floatingCharSink.of(bufPos, hi);
                            if (!Chars.utf8Decode(entity.getValue().getLo(), entity.getValue().getHi(), floatingCharSink)) {
                                throw CairoException.instance(0).put("invalid UTF8 in value for ").put(entity.getName());
                            }

                            int symIndex = tableUpdateDetails.getSymbolIndex(localDetails, colIndex, floatingCharSink);
                            if (symIndex != SymbolTable.VALUE_NOT_FOUND) {
                                bufPos = tmpBufPos;
                                Unsafe.getUnsafe().putByte(bufPos, NewLineProtoParser.ENTITY_TYPE_CACHED_TAG);
                                bufPos += Byte.BYTES;
                                Unsafe.getUnsafe().putInt(bufPos, symIndex);
                                bufPos += Integer.BYTES;
                            } else {
                                Unsafe.getUnsafe().putByte(tmpBufPos, entity.getType());
                                tmpBufPos += Byte.BYTES;
                                Unsafe.getUnsafe().putInt(tmpBufPos, l);
                                bufPos = hi;
                            }
                        } else {
                            throw CairoException.instance(0).put("queue buffer overflow");
                        }
                        break;
                    }
                    case NewLineProtoParser.ENTITY_TYPE_INTEGER:
                        Unsafe.getUnsafe().putByte(bufPos, entity.getType());
                        bufPos += Byte.BYTES;
                        Unsafe.getUnsafe().putLong(bufPos, entity.getIntegerValue());
                        bufPos += Long.BYTES;
                        break;
                    case NewLineProtoParser.ENTITY_TYPE_FLOAT:
                        Unsafe.getUnsafe().putByte(bufPos, entity.getType());
                        bufPos += Byte.BYTES;
                        Unsafe.getUnsafe().putDouble(bufPos, entity.getFloatValue());
                        bufPos += Double.BYTES;
                        break;
                    case NewLineProtoParser.ENTITY_TYPE_STRING:
                    case NewLineProtoParser.ENTITY_TYPE_SYMBOL:
                    case NewLineProtoParser.ENTITY_TYPE_LONG256: {
                        final int colTypeMeta = localDetails.getColumnTypeMeta(colIndex);
                        if (colTypeMeta == 0) { // not a geohash
                            Unsafe.getUnsafe().putByte(bufPos, entity.getType());
                            bufPos += Byte.BYTES;
                            int l = entity.getValue().length();
                            Unsafe.getUnsafe().putInt(bufPos, l);
                            bufPos += Integer.BYTES;
                            long hi = bufPos + 2L * l; // unquote
                            floatingCharSink.of(bufPos, hi);
                            if (!Chars.utf8Decode(entity.getValue().getLo(), entity.getValue().getHi(), floatingCharSink)) {
                                throw CairoException.instance(0).put("invalid UTF8 in value for ").put(entity.getName());
                            }
                            bufPos = hi;
                        } else {
                            long geohash;
                            try {
                                geohash = GeoHashes.fromStringTruncatingNl(
                                        entity.getValue().getLo(),
                                        entity.getValue().getHi(),
                                        Numbers.decodeLowShort(colTypeMeta));
                            } catch (NumericException e) {
                                geohash = GeoHashes.NULL;
                            }
                            switch (Numbers.decodeHighShort(colTypeMeta)) {
                                default:
                                    Unsafe.getUnsafe().putByte(bufPos, NewLineProtoParser.ENTITY_TYPE_GEOLONG);
                                    bufPos += Byte.BYTES;
                                    Unsafe.getUnsafe().putLong(bufPos, geohash);
                                    bufPos += Long.BYTES;
                                    break;
                                case ColumnType.GEOINT:
                                    Unsafe.getUnsafe().putByte(bufPos, NewLineProtoParser.ENTITY_TYPE_GEOINT);
                                    bufPos += Byte.BYTES;
                                    Unsafe.getUnsafe().putInt(bufPos, (int) geohash);
                                    bufPos += Integer.BYTES;
                                    break;
                                case ColumnType.GEOSHORT:
                                    Unsafe.getUnsafe().putByte(bufPos, NewLineProtoParser.ENTITY_TYPE_GEOSHORT);
                                    bufPos += Byte.BYTES;
                                    Unsafe.getUnsafe().putShort(bufPos, (short) geohash);
                                    bufPos += Short.BYTES;
                                    break;
                                case ColumnType.GEOBYTE:
                                    Unsafe.getUnsafe().putByte(bufPos, NewLineProtoParser.ENTITY_TYPE_GEOBYTE);
                                    bufPos += Byte.BYTES;
                                    Unsafe.getUnsafe().putByte(bufPos, (byte) geohash);
                                    bufPos += Byte.BYTES;
                                    break;
                            }
                        }
                        break;
                    }
                    case NewLineProtoParser.ENTITY_TYPE_BOOLEAN: {
                        Unsafe.getUnsafe().putByte(bufPos, entity.getType());
                        bufPos += Byte.BYTES;
                        Unsafe.getUnsafe().putByte(bufPos, (byte) (entity.getBooleanValue() ? 1 : 0));
                        bufPos += Byte.BYTES;
                        break;
                    }
                    case NewLineProtoParser.ENTITY_TYPE_NULL: {
                        Unsafe.getUnsafe().putByte(bufPos, entity.getType());
                        bufPos += Byte.BYTES;
                        break;
                    }
                    case NewLineProtoParser.ENTITY_TYPE_TIMESTAMP: {
                        Unsafe.getUnsafe().putByte(bufPos, entity.getType());
                        bufPos += Byte.BYTES;
                        Unsafe.getUnsafe().putLong(bufPos, entity.getTimestampValue());
                        bufPos += Long.BYTES;
                        break;
                    }
                    default:
                        // unsupported types are ignored
                        break;
                }
            } else {
                throw CairoException.instance(0).put("queue buffer overflow");
            }
        }
        threadId = tableUpdateDetails.writerThreadId;
    }

}
