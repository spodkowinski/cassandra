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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

import com.pholser.junit.quickcheck.generator.GenerationStatus;
import com.pholser.junit.quickcheck.random.SourceOfRandomness;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ColumnFamilyStoreGenerator;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy.createSSTableAndLengthPairs;
import static org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy.validateOptions;

public class SizeTieredCompactionEffectGenerator extends ColumnFamilyStoreGenerator<SizeTieredCompactionEffect>
{

    public SizeTieredCompactionEffectGenerator()
    {
        super(SizeTieredCompactionEffect.class);
    }

    public SizeTieredCompactionEffect generate(SourceOfRandomness rnd, GenerationStatus status)
    {
        SizeTieredCompactionEffect effect = new SizeTieredCompactionEffect();


        // create compaction strategy parameters
        effect.bucketLowKey = SizeTieredCompactionStrategyOptions.DEFAULT_BUCKET_LOW;
        effect.bucketHighKey = SizeTieredCompactionStrategyOptions.DEFAULT_BUCKET_HIGH;
        effect.minSSTableSize = SizeTieredCompactionStrategyOptions.DEFAULT_MIN_SSTABLE_SIZE;

        // bias options to use defaults in most cases
        if(rnd.nextDouble() > .8)
        {
            effect.bucketLowKey = rnd.nextDouble(0.01, 0.99);
            effect.bucketHighKey = rnd.nextDouble(1.01, 10.);
            effect.minSSTableSize = rnd.nextInt(0, 1000000000); // 1g
        }

        Map<String, String> options = new HashMap<>();
        options.put(SizeTieredCompactionStrategyOptions.BUCKET_LOW_KEY, String.format("%.2f", effect.bucketLowKey));
        options.put(SizeTieredCompactionStrategyOptions.BUCKET_HIGH_KEY, String.format("%.2f", effect.bucketHighKey));
        options.put(SizeTieredCompactionStrategyOptions.MIN_SSTABLE_SIZE_KEY, String.format("%d", effect.minSSTableSize));
        validateOptions(options);

        // generate CFS
        ColumnFamilyStore cfs = generateStore(rnd, status, SizeTieredCompactionStrategy.class, options);


        Function<ToLongFunction<SSTableReader>, Long > liveTableMetrics = (x) -> cfs.getLiveSSTables().stream()
                                                                              .mapToLong(x).reduce(0L, (a, b) -> a + b);

        Consumer<SizeTieredCompactionEffect.SizeTiredMetrics> updateMetrics = (metrics) -> {
            Set<SSTableReader> live = cfs.getLiveSSTables();
            metrics.numberOfSSTables = live.size();
            metrics.totalRows = liveTableMetrics.apply(SSTableReader::getTotalRows);
            metrics.bytesOnDisk = liveTableMetrics.apply(SSTable::bytesOnDisk);
            metrics.droppableTombstoneRatio = cfs.getDroppableTombstoneRatio();
            metrics.overlappingSSTables = cfs.getOverlappingSSTables(SSTableSet.LIVE, cfs.getLiveSSTables()).size();

            // create buckets related metrics
            List<List<SSTableReader>> buckets =
                SizeTieredCompactionStrategy.<SSTableReader>getBuckets(createSSTableAndLengthPairs(live),
                                                                       effect.bucketHighKey, effect.bucketLowKey, effect.minSSTableSize);
            metrics.numberOfBuckets = buckets.size();
            metrics.totalBytesOnDiskInBuckets = buckets.stream()
                                                       .flatMapToLong(bucket -> bucket.stream().mapToLong(SSTable::bytesOnDisk))
                                                       .sum();
            metrics.bucketSizes = buckets.stream().map(List::size).collect(Collectors.toList());

            List<Pair<List<SSTableReader>, Double>> bucketsWithHotness =
                SizeTieredCompactionStrategy.prunedBucketsAndHotness(buckets,
                                                                     cfs.getMinimumCompactionThreshold(),
                                                                     cfs.getMaximumCompactionThreshold());
            metrics.totalBucketHotness = bucketsWithHotness.stream().mapToDouble(s -> s.right).sum();

            // read rates for testing hotness related functions
            metrics.fifteenMinuteReadRateSum = live.stream().mapToDouble(s -> s.getReadMeter().fifteenMinuteRate()).sum();
        };

        updateMetrics.accept(effect.before);

        // trigger actual compaction
        cfs.enableAutoCompaction(true);

        updateMetrics.accept(effect.after);

        return effect;
    }

}
