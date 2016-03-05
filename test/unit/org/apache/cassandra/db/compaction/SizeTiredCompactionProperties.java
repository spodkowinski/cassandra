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

import java.math.BigInteger;
import java.util.List;

import com.google.common.collect.ImmutableSet;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;

import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.RestorableMeter;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy.createSSTableAndLengthPairs;
import static org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy.getBuckets;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


@RunWith(JUnitQuickcheck.class)
public class SizeTiredCompactionProperties
{

    @BeforeClass
    public static void prepareServer() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
    }

    @Property(trials = 50)
    public void bucketing(SizeTiredCompactionSetting setting)
    {
        List<List<Object>> buckets = getBuckets(setting.getSstables(),
                                                setting.getSizeTieredOptions().bucketHigh,
                                                setting.getSizeTieredOptions().bucketLow,
                                                setting.getSizeTieredOptions().minSSTableSize);

        assertTrue("number of buckets must be less or equal to number of sstables",
                   buckets.size() <= setting.getSstables().size());

        assertFalse("no buckets must be empty", buckets.stream().anyMatch(List::isEmpty));

        BigInteger totalSizeInBuckets = buckets.stream()
                                               .flatMap(bucket -> bucket.stream().map(setting::getSSTableSize))
                                               .map(BigInteger::valueOf)
                                               .reduce(BigInteger::add)
                                               .get();
        assertEquals("total bytes in buckets must be equal to length of sstables in input list",
                     totalSizeInBuckets, setting.getTotalSize());
    }

    @Property(trials = 10)
    public void bucketsByHotness(ColumnFamilyStore cfs, SizeTiredCompactionSetting settings) throws Exception
    {
        // test thresholds and value boundaries
        List<Pair<List<SSTableReader>, Double>> buckets = getBucketsByHotness(cfs, settings);
        int minSize = buckets.stream().mapToInt(b -> b.left.size()).min().orElse(0);
        int maxSize = buckets.stream().mapToInt(b -> b.left.size()).max().orElse(0);

        assertTrue(String.format("bucket must honor size thresholds (%d/%d)", minSize, maxSize),
                   minSize >= cfs.getMinimumCompactionThreshold() && maxSize <= cfs.getMaximumCompactionThreshold());

        double minHotness = buckets.stream().mapToDouble(b -> b.right).min().orElse(0);
        assertTrue("hotness value must be greater than zero", minHotness >= 0);

        // test rate increase -> hotness relation
        double hotness1 = getBucketsByHotness(cfs, settings).stream().mapToDouble(s -> s.right).sum();

        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            // bias towards majority of sstables being cold
            RestorableMeter meter = sstable.getReadMeter();
            sstable.overrideReadMeter(new RestorableMeter(meter.fifteenMinuteRate() * 2, meter.twoHourRate() * 2));
        }

        double hotness2 = getBucketsByHotness(cfs, settings).stream().mapToDouble(s -> s.right).sum();

        assertTrue("hotness must increase proportional with read rate",
                   (hotness1 == 0 && hotness2 == 0) || hotness2 == hotness1 * 2);
    }

    @Property(trials = 3)
    public void compactionRunEffects(ColumnFamilyStore cfs) throws Exception
    {
        int before = cfs.getLiveSSTables().size();
        // trigger actual compaction
        cfs.enableAutoCompaction(true);

        int after = cfs.getLiveSSTables().size();
        assertTrue("there must not be more sstables after compaction than before", after <= before);

        // todo: number of keys
        // number of tombstones
        // data size
    }

    @Property(trials = 3)
    public void bucketsHotnessRelation(ColumnFamilyStore cfs1, ColumnFamilyStore cfs2,
                                       SizeTiredCompactionSetting settings) throws Exception
    {
        List<Pair<List<SSTableReader>, Double>> buckets1 = getBucketsByHotness(cfs1, settings);
        List<Pair<List<SSTableReader>, Double>> buckets2 = getBucketsByHotness(cfs2, settings);
        double hotness1 = buckets1.stream().mapToDouble(s -> s.right).sum();
        int sstables1 = buckets1.stream().mapToInt(s -> s.left.size()).sum();
        double rates1 = cfs1.getLiveSSTables().stream()
                            .mapToDouble(s -> s.getReadMeter().fifteenMinuteRate()).sum();

        double hotness2 = buckets2.stream().mapToDouble(s -> s.right).sum();
        int sstables2 = buckets2.stream().mapToInt(s -> s.left.size()).sum();
        double rates2 = cfs2.getLiveSSTables().stream()
                            .mapToDouble(s -> s.getReadMeter().fifteenMinuteRate()).sum();

        String msg = "Greater total hotness must be based on higher number of sstables or read rates";
        if (hotness1 > hotness2)
            assertTrue(msg, sstables1 > sstables2 || rates1 > rates2);
        else if (hotness1 < hotness2)
            assertTrue(msg, sstables1 < sstables2 || rates1 < rates2);
    }

    private static List<Pair<List<SSTableReader>, Double>> getBucketsByHotness(ColumnFamilyStore cfs,
                                                                               SizeTiredCompactionSetting settings)
    {
        Iterable<SSTableReader> live = ImmutableSet.copyOf(cfs.getLiveSSTables());

        List<List<SSTableReader>> buckets = getBuckets(createSSTableAndLengthPairs(live),
                                                       settings.getSizeTieredOptions().bucketHigh,
                                                       settings.getSizeTieredOptions().bucketLow,
                                                       settings.getSizeTieredOptions().minSSTableSize);

        return SizeTieredCompactionStrategy.prunedBucketsAndHotness(buckets,
                                                                    cfs.getMinimumCompactionThreshold(),
                                                                    cfs.getMaximumCompactionThreshold());
    }
}
