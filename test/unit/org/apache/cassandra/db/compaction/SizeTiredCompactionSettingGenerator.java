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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import com.pholser.junit.quickcheck.generator.GenerationStatus;
import com.pholser.junit.quickcheck.generator.Generator;
import com.pholser.junit.quickcheck.random.SourceOfRandomness;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy.validateOptions;

public class SizeTiredCompactionSettingGenerator extends Generator<SizeTiredCompactionSetting>
{

    public SizeTiredCompactionSettingGenerator()
    {
        super(SizeTiredCompactionSetting.class);
    }

    public SizeTiredCompactionSetting generate(SourceOfRandomness rnd, GenerationStatus generationStatus)
    {
        return generateSetting(rnd, generationStatus);
    }

    public static SizeTiredCompactionSetting generateSetting(SourceOfRandomness rnd, GenerationStatus generationStatus)
    {
        Map<String, String> options = new HashMap<>();
        // bias options to use defaults in most cases
        if(rnd.nextDouble() > .8)
        {
            options.put(SizeTieredCompactionStrategyOptions.BUCKET_LOW_KEY,
                        String.format("%.2f", rnd.nextDouble(0.01, 0.99)));
            options.put(SizeTieredCompactionStrategyOptions.BUCKET_HIGH_KEY,
                        String.format("%.2f", rnd.nextDouble(1.01, 10.)));
            options.put(SizeTieredCompactionStrategyOptions.MIN_SSTABLE_SIZE_KEY,
                        String.format("%d", rnd.nextInt(0, 1000000000))); // 1g
            validateOptions(options);
        }

        int nfiles = rnd.nextInt(0, 100); //1000000); // 1m
        Set<Pair<Object, Long>> files = new HashSet<>(nfiles);
        Map<Object, Long> sizes = new HashMap<>(nfiles);
        BigInteger totalSize = BigInteger.ZERO;
        for(int i = 0; i < nfiles; i++)
        {
            long size = rnd.nextInt(1, 1000000); // 1mb - make sure to stay in int range!
            Pair<Object, Long> pair = Pair.create(i, size);
            files.add(pair);
            sizes.put(i, size);
            totalSize = totalSize.add(BigInteger.valueOf(size));
        }

        return new SizeTiredCompactionSetting(options, files, sizes, totalSize);
    }
}
