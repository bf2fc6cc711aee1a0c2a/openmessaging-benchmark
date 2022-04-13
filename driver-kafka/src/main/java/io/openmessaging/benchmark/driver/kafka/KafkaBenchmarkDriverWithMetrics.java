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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaBenchmarkDriverWithMetrics extends KafkaBenchmarkDriver {

    public static final String FETCH_LATENCY_AVG = "fetch-latency-avg";
    public static final String PRODUCE_THROTTLE_TIME_AVG = "produce-throttle-time-avg";
    public static final String RECORD_QUEUE_TIME_AVG = "record-queue-time-avg";
    public static final String CONNECTION_COUNT = "connection-count";

    private static final AtomicInteger CONSUMER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);

    @Override
    protected Properties newProducerProperties(String topic) {
        Properties p = super.newProducerProperties(topic);
        String clientId = "producer-"+topic+CONSUMER_CLIENT_ID_SEQUENCE.getAndIncrement();
        p.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        return p;
    }

    @Override
    protected Properties newConsumerProperties(String subscriptionName) {
        Properties p = super.newConsumerProperties(subscriptionName);
        String clientId = "consumer-"+subscriptionName+CONSUMER_CLIENT_ID_SEQUENCE.getAndIncrement();
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        return p;
    }

    @Override
    protected KafkaBenchmarkConsumer newConsumer(KafkaConsumer<String, byte[]> consumer, Properties properties,
            ConsumerCallback consumerCallback) {
        return new KafkaBenchmarkConsumerWithMetrics(consumer, properties, consumerCallback);
    }

    @Override
    protected KafkaBenchmarkProducer newProducer(KafkaProducer<String, byte[]> kafkaProducer, Properties properties, String topic) {
        return new KafkaBenchmarkProducerWithMetrics(kafkaProducer, topic, properties);
    }

}
