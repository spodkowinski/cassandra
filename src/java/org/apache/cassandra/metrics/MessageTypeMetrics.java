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
package org.apache.cassandra.metrics;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

import java.util.concurrent.ConcurrentMap;

import org.apache.cassandra.transport.Message;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import com.codahale.metrics.Meter;

/**
 * Latency metrics for covering different native transport message types.
 */
public class MessageTypeMetrics extends LatencyMetrics
{
    public static final String TYPE_NAME = "MessageType";

    public final Meter failures;

    private static final ConcurrentMap<Message.Type, MessageTypeMetrics> instances = new NonBlockingHashMap<Message.Type, MessageTypeMetrics>();


    public static MessageTypeMetrics get(Message.Type type)
    {
       MessageTypeMetrics metrics = instances.get(type);
       if (metrics == null)
       {
           metrics = new MessageTypeMetrics(type);
           instances.put(type, metrics);
       }
       return metrics;
    }

    public MessageTypeMetrics(final Message.Type type)
    {
        super(TYPE_NAME, type.name());
        failures = Metrics.meter(factory.createMetricName("Failures"));
    }

}