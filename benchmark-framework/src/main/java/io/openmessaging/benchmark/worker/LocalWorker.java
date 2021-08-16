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
package io.openmessaging.benchmark.worker;

import static java.util.stream.Collectors.toList;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.RateLimiter;

import org.HdrHistogram.Recorder;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.DefaultThreadFactory;
import io.openmessaging.benchmark.DriverConfiguration;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.utils.RandomGenerator;
import io.openmessaging.benchmark.utils.Timer;
import io.openmessaging.benchmark.utils.distributor.KeyDistributor;
import io.openmessaging.benchmark.worker.commands.ConsumerAssignment;
import io.openmessaging.benchmark.worker.commands.CountersStats;
import io.openmessaging.benchmark.worker.commands.CumulativeLatencies;
import io.openmessaging.benchmark.worker.commands.PeriodStats;
import io.openmessaging.benchmark.worker.commands.ProducerWorkAssignment;
import io.openmessaging.benchmark.worker.commands.TopicsInfo;

public class LocalWorker implements Worker, ConsumerCallback {

    private BenchmarkDriver benchmarkDriver = null;

    private List<BenchmarkProducer> producers = new ArrayList<>();
    private List<BenchmarkConsumer> consumers = new ArrayList<>();

    private final RateLimiter rateLimiter = RateLimiter.create(1.0);

    private final ExecutorService executor = Executors.newCachedThreadPool(new DefaultThreadFactory("local-worker"));

    // stats

    private final StatsLogger statsLogger;

    static class StatCounter {
        private final LongAdder total = new LongAdder();
        private volatile long last;
        private final Counter counter;

        public StatCounter(Counter counter) {
            this.counter = counter;
        }

        public void accumulate(long value) {
            total.add(value);
            counter.add(value);
        }

        public void reset() {
            total.reset();
            last = 0;
            counter.clear();
        }

        public long sinceLast() {
            long old = last;
            last = total.sum();
            return last - old;
        }

        public long getTotal() {
            return total.sum();
        }
    }

    private final StatCounter bytesSentCounter;
    private final StatCounter bytesReceivedCounter;
    private final StatCounter messagesSentCounter;
    private final StatCounter messagesReceivedCounter;
    private final StatCounter publishErrorCounter;
    private final StatCounter consumeErrorCounter;

    private final Recorder publishLatencyRecorder = new Recorder(TimeUnit.HOURS.toMicros(1), 5);
    private final Recorder cumulativePublishLatencyRecorder = new Recorder(TimeUnit.HOURS.toMicros(1), 5);
    private final OpStatsLogger publishLatencyStats;

    private final Recorder endToEndLatencyRecorder = new Recorder(TimeUnit.HOURS.toMicros(12), 5);
    private final Recorder endToEndCumulativeLatencyRecorder = new Recorder(TimeUnit.HOURS.toMicros(12), 5);
    private final OpStatsLogger endToEndLatencyStats;

    private volatile boolean testCompleted = false;

    private volatile boolean consumersArePaused = false;

    private volatile boolean producersArePaused = false;

    private volatile long lastPeriod;

    private final long startCounter = System.currentTimeMillis();

    public LocalWorker() {
        this(NullStatsLogger.INSTANCE);
    }

    public LocalWorker(StatsLogger statsLogger) {
        this.statsLogger = statsLogger;

        StatsLogger producerStatsLogger = statsLogger.scope("producer");
        this.messagesSentCounter = new StatCounter(producerStatsLogger.getCounter("messages_sent"));
        this.bytesSentCounter = new StatCounter(producerStatsLogger.getCounter("bytes_sent"));
        this.publishErrorCounter = new StatCounter(producerStatsLogger.getCounter("produce_errors"));
        this.publishLatencyStats = producerStatsLogger.getOpStatsLogger("produce_latency");

        StatsLogger consumerStatsLogger = statsLogger.scope("consumer");
        this.messagesReceivedCounter = new StatCounter(consumerStatsLogger.getCounter("messages_recv"));
        this.bytesReceivedCounter = new StatCounter(consumerStatsLogger.getCounter("bytes_recv"));
        this.consumeErrorCounter = new StatCounter(producerStatsLogger.getCounter("consume_errors"));
        this.endToEndLatencyStats = consumerStatsLogger.getOpStatsLogger("e2e_latency");
    }

    @Override
    public void initializeDriver(File driverConfigFile) throws IOException {
        Preconditions.checkArgument(benchmarkDriver == null);
        testCompleted = false;

        DriverConfiguration driverConfiguration = mapper.readValue(driverConfigFile, DriverConfiguration.class);

        log.info("Driver: {}", writer.writeValueAsString(driverConfiguration));

        try {
            benchmarkDriver = (BenchmarkDriver) Class.forName(driverConfiguration.driverClass).newInstance();
            benchmarkDriver.initialize(driverConfigFile, statsLogger);
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Topic> createTopics(TopicsInfo topicsInfo) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        Timer timer = new Timer();

        String topicPrefix = benchmarkDriver.getTopicNamePrefix();

        List<Topic> topics = new ArrayList<>();
        for (int i = 0; i < topicsInfo.numberOfTopics; i++) {
            Topic topic = new Topic(String.format("%s-%s-%04d", topicPrefix, RandomGenerator.getRandomString(), i),
                    topicsInfo.numberOfPartitionsPerTopic);
            topics.add(topic);
            futures.add(benchmarkDriver.createTopic(topic.name, topic.partitions));
        }

        futures.forEach(CompletableFuture::join);

        log.info("Created {} topics in {} ms", topics.size(), timer.elapsedMillis());
        return topics;
    }

    @Override
    public void notifyTopicCreation(List<Topic> topics) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (Topic topic : topics) {
            futures.add(benchmarkDriver.notifyTopicCreation(topic.name, topic.partitions));
        }

        futures.forEach(CompletableFuture::join);
    }

    @Override
    public void createProducers(List<String> topics) {
        Timer timer = new Timer();

        List<CompletableFuture<BenchmarkProducer>> futures = topics.stream()
                .map(topic -> benchmarkDriver.createProducer(topic)).collect(toList());

        futures.forEach(f -> producers.add(f.join()));
        log.info("Created {} producers in {} ms", producers.size(), timer.elapsedMillis());
    }

    @Override
    public void createConsumers(ConsumerAssignment consumerAssignment) {
        Timer timer = new Timer();

        List<CompletableFuture<BenchmarkConsumer>> futures = consumerAssignment.topicsSubscriptions.stream()
                .map(ts -> benchmarkDriver.createConsumer(ts.topic, ts.subscription, Optional.of(ts.partition), this))
                .collect(toList());

        futures.forEach(f -> consumers.add(f.join()));
        log.info("Created {} consumers in {} ms", consumers.size(), timer.elapsedMillis());
    }

    @Override
    public void startLoad(ProducerWorkAssignment producerWorkAssignment) {
        rateLimiter.setRate(producerWorkAssignment.publishRate);

        producers.stream().map(Collections::singletonList).forEach(producers -> submitProducersToExecutor(producers,
                KeyDistributor.build(producerWorkAssignment.keyDistributorType), producerWorkAssignment.payloadData));

        lastPeriod = System.currentTimeMillis();
    }

    @Override
    public void probeProducers() throws IOException {
        producers.forEach(producer -> producer.sendAsync(Optional.of("key"), new byte[10])
                .thenRun(() -> messagesSentCounter.accumulate(1)));
    }

    AtomicLong totalAsyncTime = new AtomicLong();
    AtomicLong sent = new AtomicLong();

    private void submitProducersToExecutor(List<BenchmarkProducer> producers, KeyDistributor keyDistributor,
            byte[] payloadData) {
        executor.submit(() -> {
            try {
                while (!testCompleted) {
                    while (producersArePaused) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                    producers.forEach(producer -> {
                        rateLimiter.acquire();
                        final long sendTime = System.nanoTime();
                        try {
                            producer.sendAsync(Optional.ofNullable(keyDistributor.next()), payloadData).handle((v, t) -> {
                                long microTime = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - sendTime);
                                if (t != null) {
                                    log.warn("Write error on message", t);
                                    publishLatencyStats.registerFailedEvent(microTime, TimeUnit.MICROSECONDS);
                                    publishErrorCounter.accumulate(1);
                                } else {
                                    messagesSentCounter.accumulate(1);
                                    bytesSentCounter.accumulate(payloadData.length);
                                    publishLatencyStats.registerSuccessfulEvent(microTime, TimeUnit.MICROSECONDS);
                                    publishLatencyRecorder.recordValue(microTime);
                                    cumulativePublishLatencyRecorder.recordValue(microTime);
                                }
                                return null;
                            });
                        } catch (Exception e) {
                            log.warn("Write error on message", e);
                            publishErrorCounter.accumulate(1);
                        }
                    });
                }
            } catch (Throwable t) {
                log.error("Got error", t);
            }
        });
    }

    @Override
    public void adjustPublishRate(double publishRate) {
        if (publishRate < 1.0) {
            rateLimiter.setRate(1.0);
            return;
        }
        rateLimiter.setRate(publishRate);
    }

    @Override
    public PeriodStats getPeriodStats() {
        PeriodStats stats = new PeriodStats();

        stats.messagesSent = messagesSentCounter.sinceLast();
        stats.bytesSent = bytesSentCounter.sinceLast();

        stats.messagesReceived = messagesReceivedCounter.sinceLast();
        stats.bytesReceived = bytesReceivedCounter.sinceLast();

        stats.publishErrors = publishErrorCounter.sinceLast();
        stats.consumerErrors = consumeErrorCounter.sinceLast();

        stats.totalMessagesSent = messagesSentCounter.getTotal();
        stats.totalMessagesReceived = messagesSentCounter.getTotal();

        stats.publishLatency = publishLatencyRecorder.getIntervalHistogram();
        stats.endToEndLatency = endToEndLatencyRecorder.getIntervalHistogram();

        long now = System.currentTimeMillis();
        stats.elapsedMillis = now - this.lastPeriod;
        this.lastPeriod = now;

        return stats;
    }

    @Override
    public CumulativeLatencies getCumulativeLatencies() {
        CumulativeLatencies latencies = new CumulativeLatencies();
        latencies.publishLatency = cumulativePublishLatencyRecorder.getIntervalHistogram();
        latencies.endToEndLatency = endToEndCumulativeLatencyRecorder.getIntervalHistogram();
        return latencies;
    }

    @Override
    public CountersStats getCountersStats() throws IOException {
        CountersStats stats = new CountersStats();
        stats.messagesSent = messagesSentCounter.getTotal();
        stats.messagesReceived = messagesReceivedCounter.getTotal();
        stats.publishErrors = publishErrorCounter.getTotal();
        stats.consumerErrors = consumeErrorCounter.getTotal();

        stats.elapsedMillis = System.currentTimeMillis() - startCounter;

        DoubleAdder latencyAvg = new DoubleAdder();
        for (BenchmarkConsumer bc : this.consumers) {
            Map<String, Object> consumerStats = bc.supplyStats();
            for (Map.Entry<String, Object> entry : consumerStats.entrySet()) {
                if (entry.getKey().equals(BenchmarkConsumer.FETCH_LATENCY_AVG)) {
                    latencyAvg.add((double)entry.getValue());
                }
            }
        }
        stats.fetchLatencyAvg = latencyAvg.doubleValue()/this.consumers.size();
        return stats;
    }

    @Override
    public void messageReceived(byte[] data, long publishTimestamp) {
        messagesReceivedCounter.accumulate(1);
        bytesReceivedCounter.accumulate(data.length);

        // NOTE: PublishTimestamp is expected to be using the wall-clock time across
        // machines
        Instant currentTime = Instant.now();
        long currentTimeNanos = TimeUnit.SECONDS.toNanos(currentTime.getEpochSecond()) + currentTime.getNano();
        long endToEndLatencyMicros = TimeUnit.NANOSECONDS.toMicros(currentTimeNanos - publishTimestamp);
        if (endToEndLatencyMicros > 0) {
            endToEndCumulativeLatencyRecorder.recordValue(endToEndLatencyMicros);
            endToEndLatencyRecorder.recordValue(endToEndLatencyMicros);
            endToEndLatencyStats.registerSuccessfulEvent(endToEndLatencyMicros, TimeUnit.MICROSECONDS);
        }

        while (consumersArePaused) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void exception(Exception e) {
        consumeErrorCounter.accumulate(1);
    }

    @Override
    public void pauseConsumers() throws IOException {
        consumersArePaused = true;
        log.info("Pausing consumers");
    }

    @Override
    public void resumeConsumers() throws IOException {
        consumersArePaused = false;
        log.info("Resuming consumers");
    }

    @Override
    public void resetStats() throws IOException {
        publishLatencyRecorder.reset();
        cumulativePublishLatencyRecorder.reset();
        endToEndLatencyRecorder.reset();
        endToEndCumulativeLatencyRecorder.reset();
    }

    @Override
    public void stopAll() throws IOException {
        testCompleted = true;
        consumersArePaused = false;
        producersArePaused = false;

        publishLatencyRecorder.reset();
        cumulativePublishLatencyRecorder.reset();
        endToEndLatencyRecorder.reset();
        endToEndCumulativeLatencyRecorder.reset();

        messagesSentCounter.reset();
        bytesSentCounter.reset();
        messagesReceivedCounter.reset();
        bytesReceivedCounter.reset();

        try {
            Thread.sleep(100);

            for (BenchmarkProducer producer : producers) {
                producer.close();
            }
            producers.clear();

            for (BenchmarkConsumer consumer : consumers) {
                consumer.close();
            }
            consumers.clear();

            if (benchmarkDriver != null) {
                benchmarkDriver.close();
                benchmarkDriver = null;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        executor.shutdown();
    }

    private static final ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    static {
        mapper.enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE);
    }

    private static final Logger log = LoggerFactory.getLogger(LocalWorker.class);

    @Override
    public void pauseProducers() throws IOException {
        producersArePaused = true;
        log.info("Pausing producers");
    }

    @Override
    public void resumeProducers() throws IOException {
        producersArePaused = false;
        log.info("Resuming producers");
    }
}
