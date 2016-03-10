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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import com.pholser.junit.quickcheck.generator.GenerationStatus;
import com.pholser.junit.quickcheck.generator.Generator;
import com.pholser.junit.quickcheck.internal.Reflection;
import com.pholser.junit.quickcheck.random.SourceOfRandomness;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LengthPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.RestorableMeter;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.MigrationManager;

public class ColumnFamilyStoreGenerator extends Generator<ColumnFamilyStore>
{


    private double tombstonesRatio = (Double) Reflection.defaultValueOf(TombstonesRatio.class, "ratio");
    private boolean noTombstones = false;
    private int minSSTables = (Integer) Reflection.defaultValueOf(SSTables.class, "min");
    private int maxSSTables = (Integer) Reflection.defaultValueOf(SSTables.class, "max");
    private int minRows = (Integer) Reflection.defaultValueOf(Rows.class, "min");
    private int maxRows = (Integer) Reflection.defaultValueOf(Rows.class, "max");

    public ColumnFamilyStoreGenerator()
    {
        super(ColumnFamilyStore.class);
    }

    public ColumnFamilyStore generate(SourceOfRandomness rnd, GenerationStatus generationStatus)
    {

        return generateSetting(rnd, generationStatus);
    }

    public ColumnFamilyStore generateSetting(SourceOfRandomness rnd, GenerationStatus generationStatus)
    {
        String sseed = String.valueOf(rnd.seed());
        sseed = sseed.replace('-', 'N'); // - not allowed as part of KS name
        String cfname = "Standard1_" + sseed + '_' + generationStatus.attempts();
        String ksname = "SizeTiredCompactionCFS_" + sseed + '_' + generationStatus.attempts();

        // TODO: add support for non-cql tables
        CFMetaData.Builder cfmBuilder = CFMetaData.Builder.create(ksname, cfname,
                                                                  false /*isDense*/, true /*isCompound*/, false /*isCounter*/)
                                                          .withPartitioner(partitioner(rnd, generationStatus));

        List<GeneratedColumn> pkColumns = columns(rnd, generationStatus, rnd.nextInt(1, 4));
        for (GeneratedColumn col : pkColumns)
        {
            cfmBuilder = cfmBuilder.addPartitionKey(ColumnIdentifier.getInterned(col.name, true), col.type);
        }

        List<GeneratedColumn> clusteringColumns = columns(rnd, generationStatus, rnd.nextInt(1, 15));
        for (GeneratedColumn col : clusteringColumns)
        {
            cfmBuilder = cfmBuilder.addClusteringColumn(ColumnIdentifier.getInterned(col.name, true), col.type);
        }

        // TODO: Cannot update both static and non-static columns with the same RowUpdateBuilder object
        List<GeneratedColumn> staticColumns = Collections.emptyList(); // columns(rnd, generationStatus, rnd.nextInt(0, 3));
        for (GeneratedColumn col : staticColumns)
        {
            cfmBuilder = cfmBuilder.addStaticColumn(ColumnIdentifier.getInterned(col.name, true), col.type);
        }

        List<GeneratedColumn> regularColumns = columns(rnd, generationStatus, rnd.nextInt(1, 15));
        for (GeneratedColumn col : regularColumns)
        {
            cfmBuilder = cfmBuilder.addRegularColumn(ColumnIdentifier.getInterned(col.name, true), col.type);
        }

        CFMetaData cfm = cfmBuilder.build();

        // set compaction strategy
        Map<String, String> thresholds = ImmutableMap.of(
                        CompactionParams.Option.MIN_THRESHOLD.toString(), Integer.toString(2),
                        CompactionParams.Option.MAX_THRESHOLD.toString(), Integer.toString(Integer.MAX_VALUE));
        cfm.compaction(CompactionParams.create(SizeTieredCompactionStrategy.class, thresholds));

        // set compression
        cfm.compression(compressionParameters(rnd, generationStatus));

        // TODO: generate keyspace params
        MigrationManager.announceNewKeyspace(KeyspaceMetadata.create(ksname, KeyspaceParams.simple(1), Tables.of(cfm)), true);

        Keyspace keyspace = Keyspace.open(ksname);
        org.apache.cassandra.db.ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        // create sstables
        int noOfSSTables = rnd.nextInt(minSSTables, maxSSTables);
        int noOfRows = rnd.nextInt(minRows, maxRows);
        for (int i = 0; i < noOfSSTables; i++)
        {
            for (int j = 0; j < noOfRows; j++)
            {
                Mutation row = row(rnd, pkColumns, clusteringColumns, regularColumns, cfs.metadata, true);
                // commit but skip commit log
                keyspace.apply(row, false);
            }
            cfs.forceBlockingFlush();
        }
        cfs.forceBlockingFlush();

        // simulate read hottness
        Set<SSTableReader> live = cfs.getLiveSSTables();
        for (SSTableReader sstable : live)
        {
            // bias towards majority of sstables being cold
            float v = rnd.nextDouble() > .8 ? rnd.nextFloat(0, 600) : 0;
            sstable.overrideReadMeter(new RestorableMeter(v, v));
        }

        int minThreshold = CompactionParams.DEFAULT_MIN_THRESHOLD;
        int maxThreshold = CompactionParams.DEFAULT_MAX_THRESHOLD;
        if (rnd.nextDouble() > .8)
        {
            minThreshold = Math.max(2, rnd.nextInt(0, live.size()));
            maxThreshold = Math.min((int) (minThreshold * rnd.nextFloat(1, 3)), live.size());
        }
        cfs.setMinimumCompactionThreshold(minThreshold);
        cfs.setMaximumCompactionThreshold(maxThreshold);

        return cfs;
    }

    private static Mutation row(SourceOfRandomness rnd,
                                List<GeneratedColumn> pkColumns,
                                List<GeneratedColumn> clusteringColumns,
                                List<GeneratedColumn> regularColumns,
                                CFMetaData metadata,
                                boolean sparse)
    {
        // create row builder with PK values
        List<Object> pkValues = pkColumns.stream().map(p -> p.value.get()).collect(Collectors.toList());
        RowUpdateBuilder rowBuilder = new RowUpdateBuilder(metadata, 0, 0L, 0, pkValues.toArray());

        // add clustering key values
        List<Object> clusteringValues = clusteringColumns.stream().map(p -> p.value.get()).collect(Collectors.toList());
        rowBuilder = rowBuilder.clustering(clusteringValues.toArray());

        // add column values
        for (GeneratedColumn col : regularColumns)
        {
            // keep some sparse columns
            if (sparse && rnd.nextDouble() > .2)
                rowBuilder = rowBuilder.add(col.name, col.value.get());
        }

        return rowBuilder.build();
    }

    public static IPartitioner partitioner(SourceOfRandomness rnd, GenerationStatus generationStatus)
    {
        double d = rnd.nextDouble();
        if (d < .8)
            return Murmur3Partitioner.instance;
        else if (d < .86)
            return ByteOrderedPartitioner.instance;
        else if (d < .90)
            return RandomPartitioner.instance;
        else if (d < .93)
            return LengthPartitioner.instance;
        else if (d < .96)
            return new LocalPartitioner(TimeUUIDType.instance);
        else
            return OrderPreservingPartitioner.instance;
    }

    public static CompressionParams compressionParameters(SourceOfRandomness rnd, GenerationStatus generationStatus)
    {
        double d = rnd.nextDouble();
        if (d < .4)
            return CompressionParams.snappy();
        else if (d < .5)
            return CompressionParams.lz4();
        else if (d < .6)
            return CompressionParams.deflate();
        else
            return CompressionParams.noCompression();
    }

    public static List<GeneratedColumn> columns(SourceOfRandomness rnd, GenerationStatus generationStatus, int length)
    {
        if (length == 0)
            return Collections.emptyList();

        return IntStream.range(0, length).mapToObj(i -> GeneratedColumn.createColumn(rnd, generationStatus))
                        .collect(Collectors.toList());
    }

    public void configure(TombstonesRatio ratio)
    {
        this.tombstonesRatio = ratio.ratio();
    }

    public void configure(NoTombstones justno)
    {
        this.noTombstones = true;
    }

    public void configure(SSTables sstables)
    {
        this.minSSTables = sstables.min();
        this.maxSSTables = sstables.max();
    }

    public void configure(Rows rows)
    {
        this.minRows = rows.min();
        this.maxRows = rows.max();
    }
}

