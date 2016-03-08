/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Date;
import java.util.UUID;
import java.util.function.Supplier;

import com.pholser.junit.quickcheck.generator.GenerationStatus;
import com.pholser.junit.quickcheck.random.SourceOfRandomness;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.serializers.AsciiSerializer;
import org.apache.cassandra.serializers.BooleanSerializer;
import org.apache.cassandra.serializers.ByteSerializer;
import org.apache.cassandra.serializers.BytesSerializer;
import org.apache.cassandra.serializers.DecimalSerializer;
import org.apache.cassandra.serializers.DoubleSerializer;
import org.apache.cassandra.serializers.FloatSerializer;
import org.apache.cassandra.serializers.InetAddressSerializer;
import org.apache.cassandra.serializers.LongSerializer;
import org.apache.cassandra.serializers.ShortSerializer;
import org.apache.cassandra.serializers.TimestampSerializer;
import org.apache.cassandra.serializers.UTF8Serializer;
import org.apache.cassandra.serializers.UUIDSerializer;


class GeneratedColumn
{
    String name;
    AbstractType type;
    Supplier<ByteBuffer> value;

    private GeneratedColumn(SourceOfRandomness rnd, GenerationStatus generationStatus)
    {
    }

    static GeneratedColumn createColumn(SourceOfRandomness rnd, GenerationStatus generationStatus)
    {
        GeneratedColumn column = new GeneratedColumn(rnd, generationStatus);
        ColumnType t = ColumnType.values()[rnd.nextInt(0, ColumnType.values().length - 1)];
        column.name = String.valueOf(rnd.nextDouble()); // TODO: string generation
        column.type = t.instance();
        column.value = t.value(rnd);
        return column;
    }

    public enum ColumnType
    {
        TInetAddress
        {
            InetAddressType instance()
            {
                return InetAddressType.instance;
            }

            Supplier<ByteBuffer> value(SourceOfRandomness rnd)
            {
                return () -> {
                    try
                    {
                        return InetAddressSerializer.instance.serialize(
                            InetAddress.getByAddress(new byte[]{ 10, (byte) rnd.nextInt(), (byte) rnd.nextInt(), (byte) rnd.nextInt() }));
                    }
                    catch (UnknownHostException e)
                    {
                        throw new RuntimeException(e);
                    }
                };
            }
        },

        TByteType
        {
            ByteType instance()
            {
                return ByteType.instance;
            }

            Supplier<ByteBuffer> value(SourceOfRandomness rnd)
            {
                return () -> ByteSerializer.instance.serialize((byte) rnd.nextInt());
            }
        },

        TLongType
        {
            LongType instance()
            {
                return LongType.instance;
            }

            Supplier<ByteBuffer> value(SourceOfRandomness rnd)
            {
                return () -> LongSerializer.instance.serialize(rnd.nextLong());
            }
        },

        TUTF8Type
        {
            AbstractType instance()
            {
                return UTF8Type.instance;
            }

            Supplier<ByteBuffer> value(SourceOfRandomness rnd)
            {

                StringBuilder builder = new StringBuilder();
                for(int i = 0; i < rnd.nextInt(1, rnd.nextInt(1, 80)); i++)
                    builder.appendCodePoint(rnd.nextInt(0, Character.MIN_SURROGATE - 1));

                return () -> UTF8Serializer.instance.serialize(builder.toString());
            }
        },

        TIntegerType
        {
            AbstractType instance()
            {
                return IntegerType.instance;
            }

            Supplier<ByteBuffer> value(SourceOfRandomness rnd)
            {
                return () -> ByteSerializer.instance.serialize((byte)rnd.nextInt());
            }
        },

        TDecimalType
        {
            AbstractType instance()
            {
                return DecimalType.instance;
            }

            Supplier<ByteBuffer> value(SourceOfRandomness rnd)
            {
                return () -> DecimalSerializer.instance.serialize(BigDecimal.valueOf(rnd.nextLong()));
            }
        },

        TTimestampType
        {
            AbstractType instance()
            {
                return TimestampType.instance;
            }

            Supplier<ByteBuffer> value(SourceOfRandomness rnd)
            {
                return () -> TimestampSerializer.instance.serialize(Date.from(Instant.ofEpochMilli(rnd.nextLong())));
            }
        },

        TDoubleType
        {
            AbstractType instance()
            {
                return DoubleType.instance;
            }

            Supplier<ByteBuffer> value(SourceOfRandomness rnd)
            {
                return () -> DoubleSerializer.instance.serialize(rnd.nextDouble());
            }
        },

        TBytesType
        {
            AbstractType instance()
            {
                return BytesType.instance;
            }

            Supplier<ByteBuffer> value(SourceOfRandomness rnd)
            {
                return () -> BytesSerializer.instance.serialize(ByteBuffer.wrap(rnd.nextBytes(rnd.nextInt(0, 100))));
            }
        },

        TBooleanType
        {
            AbstractType instance()
            {
                return BooleanType.instance;
            }

            Supplier<ByteBuffer> value(SourceOfRandomness rnd)
            {
                return () -> BooleanSerializer.instance.serialize(rnd.nextBoolean());
            }
        },

        TShortType
        {
            AbstractType instance()
            {
                return ShortType.instance;
            }

            Supplier<ByteBuffer> value(SourceOfRandomness rnd)
            {
                return () -> ShortSerializer.instance.serialize(rnd.nextShort(Short.MIN_VALUE, Short.MAX_VALUE));
            }
        },

        TUUIDType
        {
            AbstractType instance()
            {
                return UUIDType.instance;
            }

            Supplier<ByteBuffer> value(SourceOfRandomness rnd)
            {
                return () -> UUIDSerializer.instance.serialize(UUID.nameUUIDFromBytes(rnd.nextBytes(20)));
            }
        },

        TFloatType
        {
            AbstractType instance()
            {
                return FloatType.instance;
            }

            Supplier<ByteBuffer> value(SourceOfRandomness rnd)
            {
                return () -> FloatSerializer.instance.serialize(rnd.nextFloat());
            }
        },

        TAsciiType
        {
            AbstractType instance()
            {
                return AsciiType.instance;
            }

            Supplier<ByteBuffer> value(SourceOfRandomness rnd)
            {
                StringBuilder builder = new StringBuilder();
                for(int i = 0; i < rnd.nextInt(1, rnd.nextInt(1, 80)); i++)
                    builder.appendCodePoint(rnd.nextByte((byte)1, (byte)127));

                return () -> AsciiSerializer.instance.serialize(builder.toString());
            }
        };

        abstract AbstractType instance();

        abstract Supplier<ByteBuffer> value(SourceOfRandomness rnd);
    }
}
