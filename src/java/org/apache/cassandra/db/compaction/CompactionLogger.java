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

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.*;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.NoSpamLogger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import static com.google.common.io.Files.getNameWithoutExtension;

public class CompactionLogger
{
    public interface Strategy
    {
        JsonNode sstable(SSTableReader sstable);
        JsonNode options();

        static Strategy none = new Strategy()
        {
            public JsonNode sstable(SSTableReader sstable)
            {
                return null;
            }

            public JsonNode options()
            {
                return null;
            }
        };
    }

    private interface CompactionStrategyAndTableFunction
    {
        JsonNode apply(AbstractCompactionStrategy strategy, SSTableReader sstable);
    }

    private static final JsonNodeFactory json = JsonNodeFactory.instance;
    private static final Logger logger = LoggerFactory.getLogger(CompactionLogger.class);
    private static final CompactionLogSerializer serializer = new CompactionLogSerializer();
    private final ColumnFamilyStore cfs;
    private final CompactionStrategyManager csm;
    private final Map<AbstractCompactionStrategy, String> compactionStrategyMapping = new ConcurrentHashMap<>();
    private final AtomicBoolean enabled = new AtomicBoolean(false);

    public CompactionLogger(ColumnFamilyStore cfs, CompactionStrategyManager csm)
    {
        this.csm = csm;
        this.cfs = cfs;
    }

    private void forEach(Consumer<AbstractCompactionStrategy> consumer)
    {
        this.csm.getStrategies()
                .forEach(l -> l.forEach(consumer));
    }

    private ArrayNode arrayNode(Function<AbstractCompactionStrategy, JsonNode> select)
    {
        ArrayNode node = json.arrayNode();
        forEach(acs -> node.add(select.apply(acs)));
        return node;
    }

    private ArrayNode arrayNode(Collection<SSTableReader> sstables, CompactionStrategyAndTableFunction csatf)
    {
        ArrayNode node = json.arrayNode();
        sstables.forEach(t -> node.add(csatf.apply(csm.getCompactionStrategyFor(t), t)));
        return node;
    }

    private String getId(AbstractCompactionStrategy strategy)
    {
        if (strategy instanceof SizeTieredCompactionStrategy)
            return "STCS";
        else if (strategy instanceof LeveledCompactionStrategy)
            return "LCS";
        else if (strategy instanceof DateTieredCompactionStrategy)
            return "DTCS";
        else
            return strategy.getName();
    }

    private JsonNode formatSSTables(AbstractCompactionStrategy strategy)
    {
        ArrayNode node = json.arrayNode();
        for (SSTableReader sstable : this.cfs.getLiveSSTables())
        {
            if (csm.getCompactionStrategyFor(sstable) == strategy)
                node.add(formatSSTable(strategy, sstable));
        }
        return node;
    }

    private JsonNode formatSSTable(AbstractCompactionStrategy strategy, SSTableReader sstable)
    {
        ObjectNode node = json.objectNode();
        node.put("generation", sstable.descriptor.generation);
        node.put("size", sstable.onDiskLength());
        node.put("file", getNameWithoutExtension(sstable.getFilename()));
        JsonNode logResult = strategy.strategyLogger().sstable(sstable);
        if (logResult != null)
            node.put("details", logResult);
        return node;
    }

    private JsonNode start(AbstractCompactionStrategy strategy)
    {
        ObjectNode node = json.objectNode();
        node.put("id", getId(strategy));
        node.put("type", strategy.getName());
        node.put("tables", formatSSTables(strategy));
        JsonNode logResult = strategy.strategyLogger().options();
        if (logResult != null)
            node.put("options", logResult);
        return node;
    }

    private JsonNode end(AbstractCompactionStrategy strategy)
    {
        ObjectNode node = json.objectNode();
        node.put("id", getId(strategy));
        return node;
    }

    private JsonNode describe(AbstractCompactionStrategy strategy, SSTableReader sstable)
    {
        ObjectNode node = json.objectNode();
        node.put("id", getId(strategy));
        node.put("table", formatSSTable(strategy, sstable));
        return node;
    }

    private void describe(ObjectNode node)
    {
        node.put("keyspace", this.cfs.keyspace.getName());
        node.put("table", this.cfs.getTableName());
    }

    public void enable()
    {
        if (enabled.compareAndSet(false, true))
        {
            ObjectNode node = json.objectNode();
            node.put("type", "enable");
            describe(node);
            node.put("strategies", arrayNode(this::start));
            serializer.write(node);
        }
    }

    public void disable()
    {
        if (enabled.compareAndSet(true, false))
        {
            ObjectNode node = json.objectNode();
            node.put("type", "disable");
            describe(node);
            node.put("strategies", arrayNode(this::end));
            serializer.write(node);
        }
    }

    public void flush(Collection<SSTableReader> sstables)
    {
        if (enabled.get())
        {
            ObjectNode node = json.objectNode();
            node.put("type", "flush");
            describe(node);
            node.put("tables", this.arrayNode(sstables, this::describe));
            serializer.write(node);
        }
    }

    public void compaction(long startTime, Collection<SSTableReader> input, long endTime, Collection<SSTableReader> output)
    {
        if (enabled.get())
        {
            ObjectNode node = json.objectNode();
            node.put("type", "compaction");
            describe(node);
            node.put("start", DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(Instant.ofEpochMilli(startTime)
                                                                                   .atZone(ZoneId.systemDefault())));
            node.put("end", DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(Instant.ofEpochMilli(endTime)
                                                                                 .atZone(ZoneId.systemDefault())));
            node.put("input", arrayNode(input, this::describe));
            node.put("output", arrayNode(output, this::describe));
            serializer.write(node);
        }
    }

    public void pending(AbstractCompactionStrategy strategy, int remaining)
    {
        if (enabled.get() && remaining > 0)
        {
            ObjectNode node = json.objectNode();
            node.put("type", "pending");
            describe(node);
            node.put("strategy", getId(strategy));
            node.put("pending", remaining);
            serializer.write(node);
        }
    }

    private static class CompactionLogSerializer
    {
        private static final String logDirectory = System.getProperty("cassandra.logdir", ".");
        private final ExecutorService loggerService = Executors.newFixedThreadPool(1);
        private OutputStreamWriter stream;

        private static OutputStreamWriter createStream() throws IOException
        {
            int count = 0;
            Path compactionLog = Paths.get(logDirectory, "compaction.log");
            if (Files.exists(compactionLog))
            {
                Path tryPath = compactionLog;
                while (Files.exists(tryPath))
                {
                    tryPath = Paths.get(logDirectory, String.format("compaction-%d.log", count++));
                }
                Files.move(compactionLog, tryPath);
            }

            return new OutputStreamWriter(Files.newOutputStream(compactionLog, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE));
        }

        public void write(ObjectNode statement)
        {
            statement.put("date", DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now()));
            final String toWrite = statement.toString() + System.lineSeparator();
            loggerService.execute(() -> {
                try
                {
                    if (stream == null)
                        stream = createStream();

                    stream.write(toWrite);
                    stream.flush();
                }
                catch (IOException ioe)
                {
                    // We'll drop the change and log the error to the logger.
                    NoSpamLogger.log(logger, NoSpamLogger.Level.ERROR, 1, TimeUnit.MINUTES,
                                     "Could not write to the log file: {}", ioe);
                }
            });
        }
    }
}
