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

import java.util.List;

import org.junit.BeforeClass;
import org.junit.runner.RunWith;

import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.SSTable;

import static org.junit.Assert.assertTrue;


@RunWith(JUnitQuickcheck.class)
public class SizeTieredCompactionPropertiesTest
{

    @BeforeClass
    public static void prepareServer() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
    }


    @Property(trials = 3)
    public void compactionRunEffects(SizeTieredCompactionEffect effect) throws Exception
    {
        assertTrue("There must be less sstables after compaction than before",
                   effect.after.numberOfSSTables < effect.before.numberOfSSTables);


        // bucketing
        effect.trueBeforeAndAfter("Number of buckets must be less or equal to number of sstables",
                                  (metrics) -> metrics.buckets.size() <= metrics.numberOfSSTables);

        effect.trueBeforeAndAfter("No buckets must be empty",
                                  (metrics) -> !metrics.buckets.stream().anyMatch(List::isEmpty));


        effect.trueBeforeAndAfter("Total bytes in buckets must be equal to length of sstables in input list",
                                  (metrics) -> {
                                      long totalSizeInBuckets = metrics.buckets.stream()
                                                                  .flatMapToLong(bucket -> bucket.stream().mapToLong(SSTable::bytesOnDisk))
                                                                  .sum();
                                      return totalSizeInBuckets == metrics.bytesOnDisk;
                                  });



        // todo: number of keys
        // number of tombstones
        // data size
    }

    @Property(trials = 3)
    public void bucketsHotnessRelation(SizeTieredCompactionEffect effect1, SizeTieredCompactionEffect effect2) throws Exception
    {
        double hotness1 = effect1.before.bucketsByHotness.stream().mapToDouble(s -> s.right).sum();
        int sstables1 = effect1.before.numberOfSSTables;
        double rates1 = effect1.before.fifteenMinuteReadRateSum;

        double hotness2 = effect2.before.bucketsByHotness.stream().mapToDouble(s -> s.right).sum();
        int sstables2 = effect2.before.numberOfSSTables;
        double rates2 = effect2.before.fifteenMinuteReadRateSum;

        String msg = "Greater total hotness must be based on higher number of sstables or read rates";
        if (hotness1 > hotness2)
            assertTrue(msg, sstables1 > sstables2 || rates1 > rates2);
        else if (hotness1 < hotness2)
            assertTrue(msg, sstables1 < sstables2 || rates1 < rates2);
    }

}
