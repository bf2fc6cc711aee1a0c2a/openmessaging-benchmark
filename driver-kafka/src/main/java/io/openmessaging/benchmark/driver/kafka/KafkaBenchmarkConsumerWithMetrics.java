/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.openmessaging.benchmark.driver.kafka;

import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.MetricsEnabled;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.lang.management.ManagementFactory;
import java.util.Properties;
import java.util.function.BiConsumer;

public class KafkaBenchmarkConsumerWithMetrics extends KafkaBenchmarkConsumer implements MetricsEnabled {

    private String clientId;
    private MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();

    public KafkaBenchmarkConsumerWithMetrics(KafkaConsumer<String, byte[]> consumer, Properties consumerConfig,
            ConsumerCallback callback) {
        super(consumer, consumerConfig, callback);
        this.clientId = consumerConfig.getProperty(ConsumerConfig.CLIENT_ID_CONFIG);
    }

    @Override
    public void supplyMetrics(BiConsumer<String, Metric> consumer) {
        try {
            ObjectName fetchManagerName = new ObjectName(
                    "kafka.consumer:type=consumer-fetch-manager-metrics,client-id=" + this.clientId);
            Object obj = mbeanServer.getAttribute(fetchManagerName, KafkaBenchmarkDriverWithMetrics.FETCH_LATENCY_AVG);
            ObjectName consumerMetrics =
                    new ObjectName("kafka.consumer:type=consumer-metrics,client-id=" + this.clientId);
            Object objConnectionCount = mbeanServer.getAttribute(consumerMetrics, KafkaBenchmarkDriverWithMetrics.CONNECTION_COUNT);
            if (obj instanceof Double && !((Double) obj).isNaN()) {
                consumer.accept(KafkaBenchmarkDriverWithMetrics.FETCH_LATENCY_AVG, new Metric(Combiner.AVERAGE, (double)obj, "ms"));
            }
            if (objConnectionCount instanceof Double && !((Double) objConnectionCount).isNaN()) {
                consumer.accept(KafkaBenchmarkDriverWithMetrics.CONNECTION_COUNT, new Metric(Combiner.SUM, (double)objConnectionCount, "connections"));
            }
        } catch (Exception e) {
            log.error("exception fetching metrics {}", e.getMessage());
        }
    }

}
