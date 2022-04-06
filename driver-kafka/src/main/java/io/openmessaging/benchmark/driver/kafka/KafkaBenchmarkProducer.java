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

import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.MetricsEnabled;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

public class KafkaBenchmarkProducer implements BenchmarkProducer, MetricsEnabled {
    private static final Logger log = LoggerFactory.getLogger(KafkaBenchmarkProducer.class);

    private final KafkaProducer<String, byte[]> producer;
    private final String topic;
    private String clientId;
    private MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();

    public KafkaBenchmarkProducer(KafkaProducer<String, byte[]> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    public KafkaBenchmarkProducer clientId(String id) {
      this.clientId = id;
      return this;
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key.orElse(null), payload);

        CompletableFuture<Void> future = new CompletableFuture<>();

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                future.completeExceptionally(exception);
            } else {
                future.complete(null);
            }
        });

        return future;
    }

    @Override
    public Map<String, Object> supplyStats() {
        Map<String, Object> stats = new TreeMap<>();
        try {
            ObjectName fetchManagerName = new ObjectName("kafka.producer:type=producer-metrics,client-id="+this.clientId);
            Object objThrottle = mbeanServer.getAttribute(fetchManagerName, MetricsEnabled.PRODUCE_THROTTLE_TIME_AVG);
            Object objQueueTime = mbeanServer.getAttribute(fetchManagerName, MetricsEnabled.RECORD_QUEUE_TIME_AVG);
            Object objConnectionCount = mbeanServer.getAttribute(fetchManagerName, MetricsEnabled.CONNECTION_COUNT);
            if (objThrottle instanceof Double && !((Double)objThrottle).isNaN()) {
                stats.put(MetricsEnabled.PRODUCE_THROTTLE_TIME_AVG, objThrottle);
            }
            if (objQueueTime instanceof Double && !((Double)objQueueTime).isNaN()) {
                stats.put(MetricsEnabled.RECORD_QUEUE_TIME_AVG, objQueueTime);
            }
            if (objConnectionCount instanceof Double && !((Double)objConnectionCount).isNaN()) {
                stats.put(MetricsEnabled.CONNECTION_COUNT, objConnectionCount);
            }
        } catch (Exception e) {
            log.error("exception fetching 'fetch-latency-avg' metric");
        }
        return stats;
    }

    @Override
    public void close() throws Exception {
        producer.close();
    }

}
