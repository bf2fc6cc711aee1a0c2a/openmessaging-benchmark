/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openmessaging.benchmark.worker;

import static io.openmessaging.benchmark.utils.UniformRateLimiter.*;
import static java.util.stream.Collectors.toList;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.openmessaging.benchmark.utils.UniformRateLimiter;
import org.HdrHistogram.Recorder;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.collections.map.HashedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Preconditions;

import io.netty.util.concurrent.DefaultThreadFactory;
import io.openmessaging.benchmark.DriverConfiguration;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.MetricsEnabled;
import io.openmessaging.benchmark.driver.MetricsEnabled.Combiner;
import io.openmessaging.benchmark.driver.MetricsEnabled.Metric;
import io.openmessaging.benchmark.utils.RandomGenerator;
import io.openmessaging.benchmark.utils.Timer;
import io.openmessaging.benchmark.utils.distributor.KeyDistributor;
import io.openmessaging.benchmark.worker.commands.*;

public class LocalWorker implements Worker, ConsumerCallback {

    private BenchmarkDriver benchmarkDriver = null;

    private List<BenchmarkProducer> producers = new ArrayList<>();
    private List<BenchmarkConsumer> consumers = new ArrayList<>();

    private volatile UniformRateLimiter rateLimiter = new UniformRateLimiter(1.0);

    private final ExecutorService executor = Executors.newCachedThreadPool(new DefaultThreadFactory("local-worker"));

    // stats

    private final StatsLogger statsLogger;

    static class StatCounter {
        private final LongAdder total = new LongAdder();
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
            counter.clear();
        }

        public long sinceLast() {
            return total.sumThenReset();
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
    private final Recorder onDemandPublishLatencyRecorder = new Recorder(TimeUnit.HOURS.toMicros(1), 5);
    private final Recorder cumulativePublishLatencyRecorder = new Recorder(TimeUnit.HOURS.toMicros(1), 5);
    private final OpStatsLogger publishLatencyStats;

    private final Recorder publishDelayLatencyRecorder = new Recorder(TimeUnit.SECONDS.toMicros(60), 5);
    private final Recorder cumulativePublishDelayLatencyRecorder = new Recorder(TimeUnit.SECONDS.toMicros(60), 5);
    private final OpStatsLogger publishDelayLatencyStats;

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
        this.publishDelayLatencyStats = producerStatsLogger.getOpStatsLogger("producer_delay_latency");
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
    public List<String> createTopics(TopicsInfo topicsInfo) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        Timer timer = new Timer();


        List<String> topics = new ArrayList<>();
        for (int i = 0; i < topicsInfo.numberOfTopics; i++) {
            String topicPrefix = benchmarkDriver.getTopicNamePrefix();
            String topic = String.format("%s-%s-%04d", topicPrefix, RandomGenerator.getRandomString(), i);
            topics.add(topic);
            futures.add(benchmarkDriver.createTopic(topic, topicsInfo.numberOfPartitionsPerTopic));
        }

        futures.forEach(CompletableFuture::join);

        log.info("Created {} topics in {} ms", topics.size(), timer.elapsedMillis());
        return topics;
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
                .map(ts -> benchmarkDriver.createConsumer(ts.topic, ts.subscription, this)).collect(toList());

        futures.forEach(f -> consumers.add(f.join()));
        log.info("Created {} consumers in {} ms", consumers.size(), timer.elapsedMillis());
    }

    @Override
    public void startLoad(ProducerWorkAssignment producerWorkAssignment) {
        rateLimiter = new UniformRateLimiter(producerWorkAssignment.publishRate);

        // use a thread per producer - the client performs blocking actions, so we should use a high degree of concurrency here
        // TODO: cap the threads at a reasonable level
        producers.stream().map(Collections::singletonList).forEach(producers -> submitProducersToExecutor(producers,
                KeyDistributor.build(producerWorkAssignment.keyDistributorType), producerWorkAssignment.payloadData));

        lastPeriod = System.currentTimeMillis();
    }

    @Override
    public void probeProducers() throws IOException {
        producers.forEach(producer -> producer.sendAsync(Optional.of("key"), new byte[10])
                .thenRun(() -> messagesSentCounter.accumulate(1)));
    }

    private void submitProducersToExecutor(List<BenchmarkProducer> producers, KeyDistributor keyDistributor, List<byte[]> payloads) {
        executor.submit(() -> {
            int payloadCount = payloads.size();
            ThreadLocalRandom r = ThreadLocalRandom.current();
            byte[] firstPayload = payloads.get(0);

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
                        byte[] payloadData = payloadCount == 0 ? firstPayload : payloads.get(r.nextInt(payloadCount));
                        final long intendedSendTime = rateLimiter.acquire();
                        uninterruptibleSleepNs(intendedSendTime);
                        final long sendTime = System.nanoTime();
                        try {
                            producer.sendAsync(Optional.ofNullable(keyDistributor.next()), payloadData).handle((v, t) -> {
                                long microTime = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - sendTime);
                                if (t != null) {
                                    log.warn("Write error on message", t);
                                    publishLatencyStats.registerFailedEvent(microTime, TimeUnit.MICROSECONDS);
                                    publishErrorCounter.accumulate(1);
                                } else {
                                    try {
                                      messagesSentCounter.accumulate(1);
                                      bytesSentCounter.accumulate(payloadData.length);
                                      publishLatencyStats.registerSuccessfulEvent(microTime, TimeUnit.MICROSECONDS);
                                      publishLatencyRecorder.recordValue(microTime);
                                      cumulativePublishLatencyRecorder.recordValue(microTime);
                                      onDemandPublishLatencyRecorder.recordValue(microTime);

                                      final long sendDelayMicros = TimeUnit.NANOSECONDS.toMicros(sendTime - intendedSendTime);
                                      publishDelayLatencyRecorder.recordValue(sendDelayMicros);
                                      cumulativePublishDelayLatencyRecorder.recordValue(sendDelayMicros);
                                      publishDelayLatencyStats.registerSuccessfulEvent(sendDelayMicros, TimeUnit.MICROSECONDS);
                                    } catch (Exception e) {
                                      log.warn("Error capturing stats", e);
                                    }
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
        if(publishRate < 1.0) {
            rateLimiter = new UniformRateLimiter(1.0);
            return;
        }
        rateLimiter = new UniformRateLimiter(publishRate);
    }

    @Override
    public Stats getOnDemandStats() {
        Stats stats = new Stats();
        stats.publishLatency = onDemandPublishLatencyRecorder.getIntervalHistogram();
        return stats;
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
        stats.totalMessagesReceived = messagesReceivedCounter.getTotal();

        stats.publishLatency = publishLatencyRecorder.getIntervalHistogram();
        stats.publishDelayLatency = publishDelayLatencyRecorder.getIntervalHistogram();
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
        latencies.publishDelayLatency = cumulativePublishDelayLatencyRecorder.getIntervalHistogram();
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
        stats.consumers = this.consumers.size();
        stats.producers = this.producers.size();

        // collect additional metrics
        Stream<MetricsEnabled> source = Stream.concat(consumers.stream(), producers.stream())
                .filter(MetricsEnabled.class::isInstance)
                .map(MetricsEnabled.class::cast);
        processMetrics(stats, source);

        return stats;
    }

    static void processMetrics(CountersStats stats, Stream<MetricsEnabled> source) {
        Map<String, List<Metric>> metrics = new LinkedHashMap<String, List<Metric>>();

        source.forEach(m -> m.supplyMetrics((k, v) -> metrics.merge(k, new ArrayList<>(Arrays.asList(v)), (l1, l2) -> {
                    l1.addAll(l2);
                    return l1;
                })));
        metrics.entrySet().stream().forEach(e -> {
            MetricsEnabled.Combiner combiner = e.getValue().get(0).getCombiner();
            switch (combiner) {
            case AVERAGE:
                stats.additionalMetrics.put(e.getKey(),
                        new Metric(combiner,
                                e.getValue()
                                        .stream()
                                        .map(Metric::getValue)
                                        .collect(Collectors.averagingDouble(Double::doubleValue)), e.getValue().get(0).getUnits()));
                break;
            case MAX:
                stats.additionalMetrics.put(e.getKey(),
                        new Metric(combiner,
                                e.getValue()
                                        .stream()
                                        .map(Metric::getValue).max(Double::compareTo).get(), e.getValue().get(0).getUnits()));
                break;
            case SUM:
                stats.additionalMetrics.put(e.getKey(),
                        new Metric(combiner,
                                e.getValue()
                                        .stream()
                                        .map(Metric::getValue)
                                        .collect(Collectors.summingDouble(Double::doubleValue)), e.getValue().get(0).getUnits()));
                break;
            default:
                throw new IllegalStateException();
            }
        });
    }

    @Override
    public void messageReceived(byte[] data, long publishTimestamp) {
        internalMessageReceived(data.length, publishTimestamp);
    }

    @Override
    public void messageReceived(ByteBuffer data, long publishTimestamp) {
        internalMessageReceived(data.remaining(), publishTimestamp);
    }

    public void internalMessageReceived(int size, long publishTimestamp) {
        messagesReceivedCounter.accumulate(1);
        bytesReceivedCounter.accumulate(size);

        // NOTE: PublishTimestamp is using the wall-clock time across machines
        // - it was updated in later OMB versions to be millisecond, not nano, throughout
        long now = System.currentTimeMillis();
        long endToEndLatencyMicros = TimeUnit.MILLISECONDS.toMicros(now - publishTimestamp);
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
        publishDelayLatencyRecorder.reset();
        cumulativePublishDelayLatencyRecorder.reset();
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
        publishDelayLatencyRecorder.reset();
        cumulativePublishDelayLatencyRecorder.reset();
        endToEndLatencyRecorder.reset();
        endToEndCumulativeLatencyRecorder.reset();
        onDemandPublishLatencyRecorder.reset();

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
