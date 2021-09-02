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

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.MetricsEnabled;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

public class KafkaBenchmarkConsumer implements BenchmarkConsumer, MetricsEnabled {

    private static final Logger log = LoggerFactory.getLogger(KafkaBenchmarkConsumer.class);

    private final KafkaConsumer<String, byte[]> consumer;

    private final ExecutorService executor;
    private final Future<?> consumerTask;
    private volatile boolean closing = false;
    private String clientId;
    private MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();

    public KafkaBenchmarkConsumer(KafkaConsumer<String, byte[]> consumer, ConsumerCallback callback, String clientId) {
        this.consumer = consumer;
        this.executor = Executors.newSingleThreadExecutor();
        this.clientId = clientId;

        this.consumerTask = this.executor.submit(() -> {
            while (!closing) {
                try {
                    ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, byte[]> record : records) {
                        callback.messageReceived(record.value(), TimeUnit.MILLISECONDS.toNanos(record.timestamp()));
                    }

                    if (!records.isEmpty()) {
                        // Async commit all messages polled so far
                        consumer.commitAsync();
                    }
                } catch (Exception e) {
                    callback.exception(e);
                    log.error("exception occur while consuming message", e);
                }
            }
        });
    }

    @Override
    public Map<String, Object> supplyStats() {
        Map<String, Object> stats = new TreeMap<>();
        try {
            ObjectName fetchManagerName = new ObjectName("kafka.consumer:type=consumer-fetch-manager-metrics,client-id="+this.clientId);
            Object obj = mbeanServer.getAttribute(fetchManagerName, MetricsEnabled.FETCH_LATENCY_AVG);
            ObjectName consumerMetrics = new ObjectName("kafka.consumer:type=consumer-metrics,client-id="+this.clientId);
            Object objConnectionCount = mbeanServer.getAttribute(consumerMetrics, MetricsEnabled.CONNECTION_COUNT);
            if (obj instanceof Double && !((Double)obj).isNaN()) {
                stats.put(MetricsEnabled.FETCH_LATENCY_AVG, obj);
            }
            if (objConnectionCount instanceof Double  && !((Double)objConnectionCount).isNaN()) {
                stats.put(MetricsEnabled.CONNECTION_COUNT, objConnectionCount);
            }
        } catch (Exception e) {
            log.error("exception fetching metrics", e);
        }
        return stats;
    }

    @Override
    public void close() throws Exception {
        closing = true;
        executor.shutdown();
        consumerTask.get();
        consumer.close();
    }

}
