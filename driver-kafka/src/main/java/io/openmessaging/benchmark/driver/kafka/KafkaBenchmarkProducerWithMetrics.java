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

import io.openmessaging.benchmark.driver.MetricsEnabled;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.lang.management.ManagementFactory;
import java.util.Properties;
import java.util.function.BiConsumer;

public class KafkaBenchmarkProducerWithMetrics extends KafkaBenchmarkProducer implements MetricsEnabled {

    private static final Logger log = LoggerFactory.getLogger(KafkaBenchmarkProducerWithMetrics.class);

    private String clientId;
    private MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();

    public KafkaBenchmarkProducerWithMetrics(KafkaProducer<String, byte[]> producer, String topic, Properties producerConfig) {
        super(producer, topic);
        this.clientId = producerConfig.getProperty(ConsumerConfig.CLIENT_ID_CONFIG);
    }

    @Override
    public void supplyMetrics(BiConsumer<String, Metric> consumer) {
        try {
            ObjectName fetchManagerName = new ObjectName("kafka.producer:type=producer-metrics,client-id="+this.clientId);
            Object objThrottle = mbeanServer.getAttribute(fetchManagerName, KafkaBenchmarkDriverWithMetrics.PRODUCE_THROTTLE_TIME_AVG);
            Object objQueueTime = mbeanServer.getAttribute(fetchManagerName, KafkaBenchmarkDriverWithMetrics.RECORD_QUEUE_TIME_AVG);
            Object objConnectionCount = mbeanServer.getAttribute(fetchManagerName, KafkaBenchmarkDriverWithMetrics.CONNECTION_COUNT);
            if (objThrottle instanceof Double && !((Double)objThrottle).isNaN()) {
                consumer.accept(KafkaBenchmarkDriverWithMetrics.PRODUCE_THROTTLE_TIME_AVG, new Metric(Combiner.AVERAGE, (double)objThrottle, "ms"));
            }
            if (objQueueTime instanceof Double && !((Double)objQueueTime).isNaN()) {
                consumer.accept(KafkaBenchmarkDriverWithMetrics.RECORD_QUEUE_TIME_AVG, new Metric(Combiner.AVERAGE, (double)objQueueTime, "ms"));
            }
            if (objConnectionCount instanceof Double && !((Double)objConnectionCount).isNaN()) {
                consumer.accept(KafkaBenchmarkDriverWithMetrics.CONNECTION_COUNT, new Metric(Combiner.SUM, (double)objConnectionCount, "connections"));
            }
        } catch (Exception e) {
            log.error("exception fetching metrics");
        }
    }

}
