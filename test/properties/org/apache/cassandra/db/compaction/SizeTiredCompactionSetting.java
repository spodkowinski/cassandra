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
import java.util.Map;

import org.apache.cassandra.utils.Pair;

public class SizeTiredCompactionSetting
{

    private final SizeTieredCompactionStrategyOptions sizeTieredOptions;
    private final Collection<Pair<Object, Long>> sstables;
    private final Map<String, String> options;
    private final Map<Object, Long> sizes;

    private final BigInteger totalSize;

    public SizeTiredCompactionSetting(Map<String, String> options, Collection<Pair<Object, Long>> sstables,
                                      Map<Object, Long> sizes, BigInteger totalSize)
    {
        this.sizeTieredOptions = new SizeTieredCompactionStrategyOptions(options);
        this.options = options;
        this.sstables = sstables;
        this.sizes = sizes;
        this.totalSize = totalSize;
    }

    public long getSSTableSize(Object id)
    {
        return sizes.get(id);
    }


    public BigInteger getTotalSize()
    {
        return totalSize;
    }

    public SizeTieredCompactionStrategyOptions getSizeTieredOptions()
    {
        return sizeTieredOptions;
    }

    public Collection<Pair<Object, Long>> getSstables()
    {
        return sstables;
    }

    public String toString()
    {
        return "SizeTiredCompactionSetting{" +
               "sizeTieredOptions=" + options +
               ", sstables=" + sstables.size() +
               '}';
    }
}
