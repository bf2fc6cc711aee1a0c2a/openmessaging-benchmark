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
package io.openmessaging.benchmark;

import io.openmessaging.benchmark.utils.RandomGenerator;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.DefaultThreadFactory;
import io.openmessaging.benchmark.utils.PaddingDecimalFormat;
import io.openmessaging.benchmark.utils.Timer;
import io.openmessaging.benchmark.utils.payload.FilePayloadReader;
import io.openmessaging.benchmark.utils.payload.PayloadReader;
import io.openmessaging.benchmark.worker.Topic;
import io.openmessaging.benchmark.worker.Worker;
import io.openmessaging.benchmark.worker.commands.ConsumerAssignment;
import io.openmessaging.benchmark.worker.commands.CountersStats;
import io.openmessaging.benchmark.worker.commands.CumulativeLatencies;
import io.openmessaging.benchmark.worker.commands.PeriodStats;
import io.openmessaging.benchmark.worker.commands.ProducerWorkAssignment;
import io.openmessaging.benchmark.worker.commands.TopicSubscription;
import io.openmessaging.benchmark.worker.commands.TopicsInfo;

public class WorkloadGenerator implements AutoCloseable {

    public static final int STATS_PERIOD = 10_000;
    private final String driverName;
    private final Workload workload;
    private final Worker worker;

    private final ExecutorService executor = Executors
            .newCachedThreadPool(new DefaultThreadFactory("messaging-benchmark"));

    private volatile boolean runCompleted = false;
    private volatile boolean needToWaitForBacklogDraining = false;

    private volatile double targetPublishRate;

    public WorkloadGenerator(String driverName, Workload workload, Worker worker) {
        this.driverName = driverName;
        this.workload = workload;
        this.worker = worker;

        if (workload.consumerBacklogSizeGB > 0 && workload.producerRate == 0) {
            throw new IllegalArgumentException("Cannot probe producer sustainable rate when building backlog");
        }
    }

    public TestResult run() throws Exception {
        Timer timer = new Timer();
        List<Topic> topics = worker.createTopics(new TopicsInfo(workload.topics, workload.partitionsPerTopic));
        log.info("Created {} topics in {} ms", topics.size(), timer.elapsedMillis());

        // Notify other workers about these topics
        worker.notifyTopicCreation(topics);

        createProducers(topics);

        if (workload.consumerPerSubscription > 0) {
            createConsumers(topics);
            // ensureTopicsAreReady();
        }

        Future<Double> maxRateFuture = null;

        if (workload.producerRate > 0 && !workload.findSustainableRate) {
            targetPublishRate = workload.producerRate;
        } else {
            // start at the supplied rate
            targetPublishRate = workload.producerRate == 0 ? 10000 : workload.producerRate;

            maxRateFuture = executor.submit(() -> findMaximumSustainableRate(targetPublishRate));
        }

        final PayloadReader payloadReader = new FilePayloadReader(workload.messageSize);

        ProducerWorkAssignment producerWorkAssignment = new ProducerWorkAssignment();
        producerWorkAssignment.keyDistributorType = workload.keyDistributor;
        producerWorkAssignment.publishRate = targetPublishRate;
        producerWorkAssignment.payloadData = payloadReader.load(workload.payloadFile);

        worker.startLoad(producerWorkAssignment);

        if (workload.warmupDurationMinutes > 0) {
            log.info("----- Starting warm-up traffic ------");
            printAndCollectStats(workload.warmupDurationMinutes, TimeUnit.MINUTES);
            // if the backlog is decreasing, we should extend the warmup
        }

        if (workload.consumerBacklogSizeGB > 0) {
            executor.execute(() -> {
                try {
                    buildAndDrainBacklog(topics);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
        worker.resetStats();

        log.info("----- Starting benchmark traffic ------");

        TestResult result = printAndCollectStats(workload.testDurationMinutes, TimeUnit.MINUTES);
        runCompleted = true;

        try {
            worker.stopAll();
        } catch (Exception e) {
            log.error("Unable to stop workload - {}", e.toString());
        }
        if (maxRateFuture != null) {
            result.maxSustainableRate = maxRateFuture.get();
        }
        return result;
    }

    private void ensureTopicsAreReady() throws IOException {
        log.info("Waiting for consumers to be ready");
        // This is work around the fact that there's no way to have a consumer ready in
        // Kafka without first publishing
        // some message on the topic, which will then trigger the partitions assignment
        // to the consumers

        int expectedMessages = workload.topics * workload.subscriptionsPerTopic;

        // In this case we just publish 1 message and then wait for consumers to receive
        // the data
        worker.probeProducers();

        while (true) {
            CountersStats stats = worker.getCountersStats();

            if (stats.messagesReceived < expectedMessages) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } else {
                break;
            }
        }

        log.info("All consumers are ready");
    }

    /**
     * Adjust the publish rate to a level that is sustainable, meaning that we can
     * consume all the messages that are being produced
     *
     * The presumptive max should be at least twice the value of the current rate passed in
     */
    double findMaximumSustainableRate(double currentRate) throws IOException, InterruptedException {
        double maxRate = 0; // Discovered max sustainable rate

        final double maxBacklogSeconds = (workload.sustainableRateMaxBacklogMs == null ? 200 : workload.sustainableRateMaxBacklogMs)  / 1000d;

        CountersStats stats = worker.getCountersStats();

        long localTotalMessagesSentCounter = stats.messagesSent;
        long localTotalMessagesReceivedCounter = stats.messagesReceived;

        int controlPeriodMillis = 10000;
        double lastControlTimestamp = stats.elapsedMillis;

        int successfulPeriods = 0;
        boolean warming = true;
        boolean stuck = false;
        double windowStartTotalMessagesSentCounter = 0;
        double windowStartTotalMessagesReceivedCounter = 0;
        double windowStartTimestamp = 0;

        while (!runCompleted) {
            // Check every few seconds and adjust the rate
            Thread.sleep(controlPeriodMillis);

            // Consider multiple copies when using multiple subscriptions
            stats = worker.getCountersStats();
            double currentTime = stats.elapsedMillis;
            long totalMessagesSent = stats.messagesSent;
            long totalMessagesReceived = stats.messagesReceived;
            long messagesPublishedInPeriod = totalMessagesSent - localTotalMessagesSentCounter;
            long messagesReceivedInPeriod = totalMessagesReceived - localTotalMessagesReceivedCounter;
            double elapsed = (currentTime - lastControlTimestamp) / 1000d;
            double publishRateInLastPeriod = messagesPublishedInPeriod / elapsed;
            double receiveRateInLastPeriod = messagesReceivedInPeriod / elapsed;

            if (log.isDebugEnabled()) {
                log.debug(
                        "total-send: {} -- total-received: {} -- int-sent: {} -- int-received: {} -- sent-rate: {} -- received-rate: {}",
                        totalMessagesSent, totalMessagesReceived, messagesPublishedInPeriod, messagesReceivedInPeriod,
                        publishRateInLastPeriod, receiveRateInLastPeriod);
            }

            localTotalMessagesSentCounter = totalMessagesSent;
            localTotalMessagesReceivedCounter = totalMessagesReceived;
            lastControlTimestamp = currentTime;

            if (log.isDebugEnabled()) {
                log.debug("Current rate: {} -- Publish rate {} -- Consume Rate: {} -- max-rate: {}",
                        dec.format(currentRate), dec.format(publishRateInLastPeriod),
                        dec.format(receiveRateInLastPeriod), dec.format(maxRate));
            }

            if (warming) {
                warming = false;
                continue;
            }

            boolean success = true;
            // If the consumers are building backlog, we should slow down publish rate
            if (receiveRateInLastPeriod < publishRateInLastPeriod * 0.98
                    || (successfulPeriods > 0 && (localTotalMessagesReceivedCounter - windowStartTotalMessagesReceivedCounter) <
                            (localTotalMessagesSentCounter - windowStartTotalMessagesSentCounter) * .97)) {
                currentRate = (maxRate + currentRate) / 2;
                log.info("Consumers are not meeting requested rate. reducing to {}", dec.format(currentRate));
                worker.adjustPublishRate(currentRate);
                success = false;
            }

            // prevents there from being too much of a catch up
            if (workload.subscriptionsPerTopic * stats.messagesSent - stats.messagesReceived > publishRateInLastPeriod * maxBacklogSeconds) {
                // Slows the publishes to let the consumer time to absorb the backlog
                worker.adjustPublishRate(maxRate / 10);
                log.info("Working down the backlog");
                while (!runCompleted) {
                    stats = worker.getCountersStats();
                    long backlog = workload.subscriptionsPerTopic * stats.messagesSent - stats.messagesReceived;
                    if (backlog < publishRateInLastPeriod * maxBacklogSeconds / 10) {
                        break;
                    }

                    Thread.sleep(100);
                }

                if (success) {
                    if (successfulPeriods == 0) {
                        currentRate = (maxRate + 2*currentRate) / 3;
                        log.info("Resuming load at a slightly reduced rate {}", dec.format(currentRate));
                    }
                    success = false;
                }
                warming = true;
            }

            if (!success) {
                successfulPeriods = 0;
                if (maxRate/currentRate > .99) {
                    if (stuck) {
                        log.info("Not making progress, exiting");
                        runCompleted = true;
                        break;
                    }
                    stuck = true;
                    currentRate *= 1.1; // last chance
                    log.info("Adjusting rate for a final check of {}", dec.format(currentRate));
                }
            } else {
                if (successfulPeriods == 0) {
                    windowStartTotalMessagesReceivedCounter = localTotalMessagesReceivedCounter;
                    windowStartTotalMessagesSentCounter = localTotalMessagesSentCounter;
                    windowStartTimestamp = lastControlTimestamp;
                }
                if (++successfulPeriods > 2) {
                    successfulPeriods = 0;
                    double multiplier = 1.15;
                    double windowRate = (localTotalMessagesSentCounter - windowStartTotalMessagesSentCounter) * 1000d
                            / (lastControlTimestamp - windowStartTimestamp);

                    // step by a similar ratio - this logic doesn't like bursting
                    if (maxRate == 0) {
                        multiplier = 2;
                    } else if (windowRate > maxRate) {
                        multiplier = Math.max(multiplier, windowRate/maxRate);
                    }

                    if (windowRate > maxRate) {
                        maxRate = windowRate;
                        stuck = false;
                        log.info("At max {}", dec.format(maxRate));
                    } else if (windowRate < .9*currentRate) {
                        throw new IllegalStateException(String.format(
                                "The effective rate %s is lower than expected %s.  "
                                + "The producers are not generating the expected load or something is wrong.",
                                dec.format(windowRate), dec.format(currentRate)));
                    }
                    currentRate = maxRate * multiplier;
                    log.info("Adjusting rate to {}", dec.format(currentRate));
                }
            }

            worker.adjustPublishRate(currentRate);
        }

        return maxRate;
    }

    @Override
    public void close() throws Exception {
        worker.stopAll();
        executor.shutdownNow();
    }

    private void createConsumers(List<Topic> topics) throws IOException {
        ConsumerAssignment consumerAssignment = new ConsumerAssignment();

        for (Topic topic : topics) {
            for (int i = 0; i < workload.subscriptionsPerTopic; i++) {
                String subscriptionName = String.format("sub-%03d-%s", i, RandomGenerator.getRandomString());
                for (int j = 0; j < workload.consumerPerSubscription; j++) {
                    consumerAssignment.topicsSubscriptions.add(new TopicSubscription(topic.name, subscriptionName, j));
                }
            }
        }

        Collections.shuffle(consumerAssignment.topicsSubscriptions);

        Timer timer = new Timer();

        worker.createConsumers(consumerAssignment);
        log.info("Created {} consumers in {} ms", consumerAssignment.topicsSubscriptions.size(), timer.elapsedMillis());
    }

    private void createProducers(List<Topic> topics) throws IOException {
        List<String> fullListOfTopics = new ArrayList<>();

        // Add the topic multiple times, one for each producer
        for (int i = 0; i < workload.producersPerTopic; i++) {
            topics.forEach(topic -> fullListOfTopics.add(topic.name));
        }

        Collections.shuffle(fullListOfTopics);

        Timer timer = new Timer();

        worker.createProducers(fullListOfTopics);
        log.info("Created {} producers in {} ms", fullListOfTopics.size(), timer.elapsedMillis());
    }

    private void buildAndDrainBacklog(List<Topic> topics) throws IOException {
        log.info("Stopping all consumers to build backlog");
        worker.pauseConsumers();

        this.needToWaitForBacklogDraining = true;

        long requestedBacklogSize = workload.consumerBacklogSizeGB * 1024 * 1024 * 1024;

        while (true) {
            CountersStats stats = worker.getCountersStats();
            long currentBacklogSize = (workload.subscriptionsPerTopic * stats.messagesSent - stats.messagesReceived)
                    * workload.messageSize;

            if (currentBacklogSize >= requestedBacklogSize) {
                break;
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        log.info("--- Start draining backlog ---");

        if (workload.consumerOnly) {
            log.info("Consume only test. Pausing producers while backlog is drained");
            worker.pauseProducers();
        }
        worker.resumeConsumers();

        final long minBacklog = 1000;

        while (true) {
            CountersStats stats = worker.getCountersStats();
            long currentBacklog = workload.subscriptionsPerTopic * stats.messagesSent - stats.messagesReceived;
            if (currentBacklog <= minBacklog) {
                log.info("--- Completed backlog draining ---");
                needToWaitForBacklogDraining = false;
                return;
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private TestResult printAndCollectStats(long testDurations, TimeUnit unit) throws IOException {
        long startTime = System.nanoTime();

        // Print report stats

        long testEndTime = testDurations > 0 ? startTime + unit.toNanos(testDurations) : Long.MAX_VALUE;

        TestResult result = new TestResult();
        result.workload = workload.name;
        result.driver = driverName;

        long due = System.currentTimeMillis() + STATS_PERIOD;
        while (!runCompleted) {
            try {
                long remaining = due - System.currentTimeMillis();
                if (remaining > 0) {
                    Thread.sleep(remaining);
                }
                due = System.currentTimeMillis() + STATS_PERIOD;
            } catch (InterruptedException e) {
                break;
            }

            PeriodStats stats = worker.getPeriodStats();
            CountersStats counterStats = worker.getCountersStats();

            long now = System.nanoTime();
            double elapsed = stats.elapsedMillis / 1000d;

            double publishRate = stats.messagesSent / elapsed;
            double publishThroughput = stats.bytesSent / elapsed / 1024 / 1024;

            double consumeRate = stats.messagesReceived / elapsed;
            double consumeThroughput = stats.bytesReceived / elapsed / 1024 / 1024;

            long currentBacklog = workload.subscriptionsPerTopic * stats.totalMessagesSent
                    - stats.totalMessagesReceived;

            log.info(
                    "Pub rate {} msg/s / {} MB/s | Cons rate {} msg/s / {} MB/s |Consumer Latency (ms) avg: {} | Backlog: {} K | Pub Latency (ms) avg: {} - 50%: {} - 99%: {} - 99.9%: {} - Max: {}",
                    rateFormat.format(publishRate), throughputFormat.format(publishThroughput),
                    rateFormat.format(consumeRate), throughputFormat.format(consumeThroughput),
                    dec.format(counterStats.fetchLatencyAvg),
                    dec.format(currentBacklog / 1000.0), //
                    dec.format(microsToMillis(stats.publishLatency.getMean())),
                    dec.format(microsToMillis(stats.publishLatency.getValueAtPercentile(50))),
                    dec.format(microsToMillis(stats.publishLatency.getValueAtPercentile(99))),
                    dec.format(microsToMillis(stats.publishLatency.getValueAtPercentile(99.9))),
                    throughputFormat.format(microsToMillis(stats.publishLatency.getMaxValue())));

            log.info("E2E Latency (ms) avg: {} - 50%: {} - 99%: {} - 99.9%: {} - Max: {}",
                    dec.format(microsToMillis(stats.endToEndLatency.getMean())),
                    dec.format(microsToMillis(stats.endToEndLatency.getValueAtPercentile(50))),
                    dec.format(microsToMillis(stats.endToEndLatency.getValueAtPercentile(99))),
                    dec.format(microsToMillis(stats.endToEndLatency.getValueAtPercentile(99.9))),
                    throughputFormat.format(microsToMillis(stats.endToEndLatency.getMaxValue())));

            result.publishRate.add(publishRate);
            result.consumeRate.add(consumeRate);
            result.backlog.add(currentBacklog);
            result.publishLatencyAvg.add(microsToMillis(stats.publishLatency.getMean()));
            result.publishLatency50pct.add(microsToMillis(stats.publishLatency.getValueAtPercentile(50)));
            result.publishLatency75pct.add(microsToMillis(stats.publishLatency.getValueAtPercentile(75)));
            result.publishLatency95pct.add(microsToMillis(stats.publishLatency.getValueAtPercentile(95)));
            result.publishLatency99pct.add(microsToMillis(stats.publishLatency.getValueAtPercentile(99)));
            result.publishLatency999pct.add(microsToMillis(stats.publishLatency.getValueAtPercentile(99.9)));
            result.publishLatency9999pct.add(microsToMillis(stats.publishLatency.getValueAtPercentile(99.99)));
            result.publishLatencyMax.add(microsToMillis(stats.publishLatency.getMaxValue()));

            result.endToEndLatencyAvg.add(microsToMillis(stats.endToEndLatency.getMean()));
            result.endToEndLatency50pct.add(microsToMillis(stats.endToEndLatency.getValueAtPercentile(50)));
            result.endToEndLatency75pct.add(microsToMillis(stats.endToEndLatency.getValueAtPercentile(75)));
            result.endToEndLatency95pct.add(microsToMillis(stats.endToEndLatency.getValueAtPercentile(95)));
            result.endToEndLatency99pct.add(microsToMillis(stats.endToEndLatency.getValueAtPercentile(99)));
            result.endToEndLatency999pct.add(microsToMillis(stats.endToEndLatency.getValueAtPercentile(99.9)));
            result.endToEndLatency9999pct.add(microsToMillis(stats.endToEndLatency.getValueAtPercentile(99.99)));
            result.endToEndLatencyMax.add(microsToMillis(stats.endToEndLatency.getMaxValue()));

            if (now >= testEndTime && !needToWaitForBacklogDraining) {
                boolean complete = false;
                int retry = 0;
                CumulativeLatencies agg = null;
                do {
                    try {
                        agg = worker.getCumulativeLatencies();
                    } catch (Exception e) {
                        log.info("Retrying");
                        retry++;
                        continue;
                    }
                    complete = true;
                } while (!complete && retry < 10);

                if (!complete) {
                    throw new RuntimeException("Failed to collect aggregate latencies");
                }

                log.info(
                        "----- Aggregated Pub Latency (ms) avg: {} - 50%: {} - 95%: {} - 99%: {} - 99.9%: {} - 99.99%: {} - Max: {}",
                        dec.format(microsToMillis(agg.publishLatency.getMean())),
                        dec.format(microsToMillis(agg.publishLatency.getValueAtPercentile(50))),
                        dec.format(microsToMillis(agg.publishLatency.getValueAtPercentile(95))),
                        dec.format(microsToMillis(agg.publishLatency.getValueAtPercentile(99))),
                        dec.format(microsToMillis(agg.publishLatency.getValueAtPercentile(99.9))),
                        dec.format(microsToMillis(agg.publishLatency.getValueAtPercentile(99.99))),
                        throughputFormat.format(microsToMillis(agg.publishLatency.getMaxValue())));

                result.aggregatedPublishLatencyAvg = microsToMillis(agg.publishLatency.getMean());
                result.aggregatedPublishLatency50pct = microsToMillis(agg.publishLatency.getValueAtPercentile(50));
                result.aggregatedPublishLatency75pct = microsToMillis(agg.publishLatency.getValueAtPercentile(75));
                result.aggregatedPublishLatency95pct = microsToMillis(agg.publishLatency.getValueAtPercentile(95));
                result.aggregatedPublishLatency99pct = microsToMillis(agg.publishLatency.getValueAtPercentile(99));
                result.aggregatedPublishLatency999pct = microsToMillis(agg.publishLatency.getValueAtPercentile(99.9));
                result.aggregatedPublishLatency9999pct = microsToMillis(agg.publishLatency.getValueAtPercentile(99.99));
                result.aggregatedPublishLatencyMax = microsToMillis(agg.publishLatency.getMaxValue());

                result.aggregatedEndToEndLatencyAvg = microsToMillis(agg.endToEndLatency.getMean());
                result.aggregatedEndToEndLatency50pct = microsToMillis(agg.endToEndLatency.getValueAtPercentile(50));
                result.aggregatedEndToEndLatency75pct = microsToMillis(agg.endToEndLatency.getValueAtPercentile(75));
                result.aggregatedEndToEndLatency95pct = microsToMillis(agg.endToEndLatency.getValueAtPercentile(95));
                result.aggregatedEndToEndLatency99pct = microsToMillis(agg.endToEndLatency.getValueAtPercentile(99));
                result.aggregatedEndToEndLatency999pct = microsToMillis(agg.endToEndLatency.getValueAtPercentile(99.9));
                result.aggregatedEndToEndLatency9999pct = microsToMillis(
                        agg.endToEndLatency.getValueAtPercentile(99.99));
                result.aggregatedEndToEndLatencyMax = microsToMillis(agg.endToEndLatency.getMaxValue());

                agg.publishLatency.percentiles(100).forEach(value -> {
                    result.aggregatedPublishLatencyQuantiles.put(value.getPercentile(),
                            microsToMillis(value.getValueIteratedTo()));
                });

                agg.endToEndLatency.percentiles(100).forEach(value -> {
                    result.aggregatedEndToEndLatencyQuantiles.put(value.getPercentile(),
                            microsToMillis(value.getValueIteratedTo()));
                });

                break;
            }
        }

        return result;
    }

    private static double microsToMillis(double microTime) {
        return microTime / (1000);
    }

    private static double microsToMillis(long microTime) {
        return microTime / (1000.0);
    }

    private static final DecimalFormat rateFormat = new PaddingDecimalFormat("0.000", 7);
    private static final DecimalFormat throughputFormat = new PaddingDecimalFormat("0.000", 4);
    private static final DecimalFormat dec = new PaddingDecimalFormat("0.000", 4);

    private static final Logger log = LoggerFactory.getLogger(WorkloadGenerator.class);
}
