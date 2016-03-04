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

import org.apache.cassandra.db.ColumnFamilyStore;

public class SizeTiredCompactionCFS
{
    private final SizeTiredCompactionSetting setting;
    private final ColumnFamilyStore cfs;
    private final int maxThreshold;
    private final int minThreshold;

    public SizeTiredCompactionCFS(SizeTiredCompactionSetting setting, ColumnFamilyStore cfs,
                                  int minThreshold, int maxThreshold)
    {
        this.setting = setting;
        this.cfs = cfs;
        this.minThreshold = minThreshold;
        this.maxThreshold = maxThreshold;
    }

    public ColumnFamilyStore getColumnFamilyStore()
    {
        return cfs;
    }

    public SizeTiredCompactionSetting getSetting()
    {
        return setting;
    }

    public int getMaxThreshold()
    {
        return maxThreshold;
    }

    public int getMinThreshold()
    {
        return minThreshold;
    }

    public String toString()
    {
        return "SizeTiredCompactionCFS{" +
               "setting=" + setting +
               ", cfs=" + cfs +
               ", minThreshold=" + minThreshold +
               ", maxThreshold=" + maxThreshold +
               '}';
    }
}
