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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.function.Supplier;

import com.pholser.junit.quickcheck.generator.GenerationStatus;
import com.pholser.junit.quickcheck.random.SourceOfRandomness;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.InetAddressType;


class GeneratedColumn
{
    String name;
    AbstractType type;
    Supplier<?> value;

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

            Supplier<InetAddress> value(SourceOfRandomness rnd)
            {
                return () -> {
                    try
                    {
                        return InetAddress.getByAddress(new byte[]{ 10, (byte) rnd.nextInt(),
                                                                    (byte) rnd.nextInt(), (byte) rnd.nextInt() });
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

            Supplier<?> value(SourceOfRandomness rnd)
            {
                return () -> (byte) rnd.nextInt();
            }
        };

        abstract AbstractType instance();

        abstract Supplier<?> value(SourceOfRandomness rnd);
    }
}
