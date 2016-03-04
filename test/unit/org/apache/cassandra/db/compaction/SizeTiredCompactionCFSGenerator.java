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

package org.apache.cassandra.db.compaction;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

import com.pholser.junit.quickcheck.generator.GenerationStatus;
import com.pholser.junit.quickcheck.generator.Generator;
import com.pholser.junit.quickcheck.random.SourceOfRandomness;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.RestorableMeter;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.Pair;

public class SizeTiredCompactionCFSGenerator extends Generator<SizeTiredCompactionCFS>
{


    public SizeTiredCompactionCFSGenerator(Class<SizeTiredCompactionCFS> type)
    {
        super(type);
    }

    public SizeTiredCompactionCFSGenerator(List<Class<SizeTiredCompactionCFS>> types)
    {
        super(types);
    }

    public SizeTiredCompactionCFS generate(SourceOfRandomness rnd, GenerationStatus generationStatus)
    {

        return generateSetting(rnd, generationStatus);
    }

    public static SizeTiredCompactionCFS generateSetting(SourceOfRandomness rnd, GenerationStatus generationStatus)
    {

        long ts = System.nanoTime();
        String cfname = "Standard1_" + generationStatus.attempts() + '_' + ts;
        String ksname = "SizeTiredCompactionCFS_" + generationStatus.attempts() + '_' + ts;
        Map<String, String> thresholds =
        ImmutableMap.of(CompactionParams.Option.MIN_THRESHOLD.toString(), Integer.toString(4),
                        CompactionParams.Option.MAX_THRESHOLD.toString(), Integer.toString(32));
        SchemaLoader.createKeyspace(ksname,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(ksname, cfname)
                                                .compaction(CompactionParams.create(SizeTieredCompactionStrategy.class,
                                                                                    thresholds)));

        SizeTiredCompactionSetting setting = SizeTiredCompactionSettingGenerator.generateSetting(rnd, generationStatus);
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();
//        cfs.setMaximumCompactionThreshold(0);
//        cfs.setMinimumCompactionThreshold(0);

        // create sstables
        for (Pair<Object, Long> sstable : setting.getSstables())
        {
            ByteBuffer value = ByteBuffer.wrap(new byte[(int) setting.getSSTableSize(sstable.left)]);
            new RowUpdateBuilder(cfs.metadata, 0, sstable.left.toString())
            .clustering("column").add("val", value)
            .build().applyUnsafe();
            cfs.forceBlockingFlush();
        }
        cfs.forceBlockingFlush();

        Set<SSTableReader> live = cfs.getLiveSSTables();
        assert setting.getSstables().size() == live.size();

        // simulate read hottness
        for (SSTableReader sstable : live)
        {
            // bias towards majority of sstables being cold
            float v = rnd.nextDouble() > .8 ? rnd.nextFloat(0, 600) : 0;
            sstable.overrideReadMeter(new RestorableMeter(v, v));
        }

        int minThreshold = rnd.nextInt(0, live.size());
        int maxThreshold = Math.min((int) (minThreshold * rnd.nextFloat(1, 3)), live.size());

        return new SizeTiredCompactionCFS(setting, cfs, minThreshold, maxThreshold);
    }
}
