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
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertTrue;

public class SizeTieredCompactionEffect
{

    public SizeTiredMetrics before = new SizeTiredMetrics();
    public SizeTiredMetrics after = new SizeTiredMetrics();
    public double bucketLowKey;
    public double bucketHighKey;
    public long minSSTableSize;


    public void trueBeforeAndAfter(String msg, Predicate<SizeTiredMetrics> check) {
        assertTrue(msg, check.test(before));
        assertTrue(msg, check.test(after));
    };


    public static class SizeTiredMetrics extends CompactionMetrics {


        public double fifteenMinuteReadRateSum;
        public long totalBytesOnDiskInBuckets;
        public double totalBucketHotness;
        public int numberOfBuckets;
        public List<Integer> bucketSizes;
    }
}
