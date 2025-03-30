/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.consumer;


import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.TestUtils;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for the consumer that covers the poll logic
 */
@Timeout(600)
@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = 3,
    serverProperties = {
        @ClusterConfigProperty(key = OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "3"),
        @ClusterConfigProperty(key = OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, value = "100"),
        @ClusterConfigProperty(key = GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, value = "60000"),
        @ClusterConfigProperty(key = GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, value = "10"),
        @ClusterConfigProperty(key = CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, value = "500"),
        @ClusterConfigProperty(key = CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG, value = "500"),
    }
)
public class PlaintextConsumerPollTest {
    private static final String TOPIC_1 = "TOPIC_1";
    private static final String TOPIC_2 = "TOPIC_2";
    private static final int PARTITION = 0;
    private final TopicPartition topicPartition1 = new TopicPartition(TOPIC_1, PARTITION);
    private final TopicPartition topicPartition2 = new TopicPartition(TOPIC_2, PARTITION);
    private final ClusterInstance cluster;

    public PlaintextConsumerPollTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @BeforeEach
    void setUp() {
        try (Admin admin = cluster.admin()) {
            admin.createTopics(List.of(new NewTopic(TOPIC_1, 2, (short) 3)));
        }
    }

    @ClusterTest
    void testClassicConsumerMaxPollRecords() throws InterruptedException {
        testMaxPollRecords(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    void testAsyncConsumerMaxPollRecords() throws InterruptedException {
        testMaxPollRecords(GroupProtocol.CONSUMER);
    }

    private void testMaxPollRecords(GroupProtocol groupProtocol) throws InterruptedException {
        int maxPollRecords = 2;
        int numberOfRecords = 10000;
        long startingTimestamp = System.currentTimeMillis();
        sendRecords(numberOfRecords, topicPartition1, startingTimestamp);
        Map<String, Object> consumerConfigs = Map.of(
            GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT),
            ENABLE_AUTO_COMMIT_CONFIG, false,
            MAX_POLL_RECORDS_CONFIG, maxPollRecords
        );
        try (Consumer<byte[], byte[]> consumer = cluster.consumer(consumerConfigs)) {
            consumer.assign(List.of(topicPartition1));
            consumeAndVerifyRecords(consumer,
                numberOfRecords,
                0,
                0,
                startingTimestamp,
                TimestampType.CREATE_TIME,
                topicPartition1,
                maxPollRecords);
        }
    }

    @ClusterTest
    void testClassicConsumerMaxPollIntervalMs() throws InterruptedException {
        testMaxPollIntervalMs(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    void testAsyncConsumerMaxPollIntervalMs() throws InterruptedException {
        testMaxPollIntervalMs(GroupProtocol.CONSUMER);
    }

    private void testMaxPollIntervalMs(GroupProtocol groupProtocol) throws InterruptedException {
        HashMap<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "1000");
        consumerConfig.put(GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT));
        if (GroupProtocol.CLASSIC == groupProtocol) {
            consumerConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "500");
            consumerConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "2000");
        }

        try (Consumer<byte[], byte[]> consumer = cluster.consumer(consumerConfig)) {
            TestConsumerReassignmentListener listener = new TestConsumerReassignmentListener();
            consumer.subscribe(List.of(TOPIC_1), listener);

            // rebalance to get the initial assignment
            awaitRebalance(consumer, listener);
            assertEquals(1, listener.callsToAssigned());
            assertEquals(0, listener.callsToRevoked());

            // after we extend longer than max.poll a rebalance should be triggered
            // NOTE we need to have a relatively much larger value than max.poll to let heartbeat expired for sure
            Thread.sleep(3000);

            awaitRebalance(consumer, listener);
            assertEquals(2, listener.callsToAssigned());
            assertEquals(1, listener.callsToRevoked());
        }
    }

    @ClusterTest
    void testClassicConsumerMaxPollIntervalMsDelayInRevocation() throws InterruptedException {
        testMaxPollIntervalMsDelayInRevocation(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    void testAsyncConsumerMaxPollIntervalMsDelayInRevocation() throws InterruptedException {
        testMaxPollIntervalMsDelayInRevocation(GroupProtocol.CONSUMER);
    }

    private void testMaxPollIntervalMsDelayInRevocation(GroupProtocol groupProtocol) throws InterruptedException {
        HashMap<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "5000");
        consumerConfig.put(ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerConfig.put(GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT));
        if (GroupProtocol.CLASSIC == groupProtocol) {
            consumerConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "500");
            consumerConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "2000");
        }

        try (Consumer<byte[], byte[]> consumer = cluster.consumer(consumerConfig)) {
            TestDelayInRevocationConsumerReassignmentListener listener = new TestDelayInRevocationConsumerReassignmentListener(consumer, topicPartition1);
            consumer.subscribe(List.of(TOPIC_1), listener);

            // rebalance to get the initial assignment
            awaitRebalance(consumer, listener);

            // force a rebalance to trigger an invocation of the revocation callback while in the group
            consumer.subscribe(List.of("otherTopic"), listener);
            awaitRebalance(consumer, listener);

            assertEquals(0, listener.committedPosition());
            assertTrue(listener.commitCompleted());
        }
    }

    @ClusterTest
    void testClassicConsumerMaxPollIntervalMsDelayInAssignment() throws InterruptedException {
        testMaxPollIntervalMsDelayInAssignment(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    void testAsyncConsumerMaxPollIntervalMsDelayInAssignment() throws InterruptedException {
        testMaxPollIntervalMsDelayInAssignment(GroupProtocol.CONSUMER);
    }

    private void testMaxPollIntervalMsDelayInAssignment(GroupProtocol groupProtocol) throws InterruptedException {
        HashMap<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "5000");
        consumerConfig.put(ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerConfig.put(GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT));
        if (GroupProtocol.CLASSIC == groupProtocol) {
            consumerConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "500");
            consumerConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "1000");
        }

        try (Consumer<byte[], byte[]> consumer = cluster.consumer(consumerConfig)) {
            TestConsumerReassignmentListener listener = new TestConsumerReassignmentListener() {
                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    Utils.sleep(1500);
                    super.onPartitionsAssigned(partitions);
                }
            };

            consumer.subscribe(List.of(TOPIC_1), listener);

            // rebalance to get the initial assignment
            awaitRebalance(consumer, listener);

            // We should still be in the group after this invocation
            ensureNoRebalance(consumer, listener);
        }
    }

    @ClusterTest
    void testClassicConsumerMaxPollIntervalMsShorterThanPollTimeout() throws InterruptedException {
        testMaxPollIntervalMsShorterThanPollTimeout(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    void testAsyncConsumerMaxPollIntervalMsShorterThanPollTimeout() throws InterruptedException {
        testMaxPollIntervalMsShorterThanPollTimeout(GroupProtocol.CONSUMER);
    }

    private void testMaxPollIntervalMsShorterThanPollTimeout(GroupProtocol groupProtocol) throws InterruptedException {
        HashMap<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "1000");
        consumerConfig.put(GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT));
        if (GroupProtocol.CLASSIC == groupProtocol) {
            consumerConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "500");
        }

        try (Consumer<byte[], byte[]> consumer = cluster.consumer(consumerConfig)) {
            TestConsumerReassignmentListener listener = new TestConsumerReassignmentListener();
            consumer.subscribe(List.of(TOPIC_1), listener);

            // rebalance to get the initial assignment
            awaitRebalance(consumer, listener);
            int callsToAssignedAfterFirstRebalance = listener.callsToAssigned();

            consumer.poll(Duration.ofMillis(2000));
            // If the poll above times out, it would trigger a rebalance.
            // Leave some time for the rebalance to happen and check for the rebalance event.
            consumer.poll(Duration.ofMillis(500));
            consumer.poll(Duration.ofMillis(500));

            assertEquals(callsToAssignedAfterFirstRebalance, listener.callsToAssigned());
        }
    }

    @ClusterTest
    void testClassicConsumerPerPartitionLeadWithMaxPollRecords() throws InterruptedException {
        testPerPartitionLeadWithMaxPollRecords(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    void testAsyncConsumerPerPartitionLeadWithMaxPollRecords() throws InterruptedException {
        testPerPartitionLeadWithMaxPollRecords(GroupProtocol.CONSUMER);
    }

    private void testPerPartitionLeadWithMaxPollRecords(GroupProtocol groupProtocol) throws InterruptedException {
        int numberOfMessage = 1000;
        int maxPollRecords = 10;
        sendRecords(numberOfMessage, topicPartition1, System.currentTimeMillis());

        Map<String, Object> consumerConfigs = Map.of(
            MAX_POLL_RECORDS_CONFIG, maxPollRecords,
            CLIENT_ID_CONFIG, "testPerPartitionLeadWithMaxPollRecords",
            GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT)
        );
        try (Consumer<byte[], byte[]> consumer = cluster.consumer(consumerConfigs)) {
            consumer.assign(List.of(topicPartition1));
            TestUtils.waitForCondition(() -> !consumer.poll(Duration.ofMillis(100)).isEmpty(),
                15000,
                "Consumer did not consume any messages for partition " + topicPartition1 + " before timeout.",
                OptionalLong.of(0));

            Map<String, String> tags = Map.of(
                "client-id", "testPerPartitionLeadWithMaxPollRecords",
                "topic", topicPartition1.topic(),
                "partition", String.valueOf(topicPartition1.partition())
            );

            Metric lead = consumer.metrics().get(new MetricName("records-lead", "consumer-fetch-manager-metrics", "", tags));
            assertEquals(maxPollRecords, ((double) lead.metricValue()), "The lead should be " + maxPollRecords);
        }
    }

    @ClusterTest
    void testClassicConsumerMultiConsumerSessionTimeoutOnStopPolling() throws InterruptedException, ExecutionException {
        runMultiConsumerSessionTimeoutTest(GroupProtocol.CLASSIC, false);
    }

    @ClusterTest
    void testAsyncConsumerMultiConsumerSessionTimeoutOnStopPolling() throws InterruptedException, ExecutionException {
        runMultiConsumerSessionTimeoutTest(GroupProtocol.CONSUMER, false);
    }

    @ClusterTest
    void testClassicConsumerMultiConsumerSessionTimeoutOnClose() throws InterruptedException, ExecutionException {
        runMultiConsumerSessionTimeoutTest(GroupProtocol.CLASSIC, true);
    }

    @ClusterTest
    void testAsyncConsumerMultiConsumerSessionTimeoutOnClose() throws InterruptedException, ExecutionException {
        runMultiConsumerSessionTimeoutTest(GroupProtocol.CONSUMER, true);
    }

    private void runMultiConsumerSessionTimeoutTest(GroupProtocol groupProtocol, boolean closeConsumer) throws InterruptedException, ExecutionException {
        List<ConsumerAssignmentPoller> consumerPollers = new ArrayList<>();
        try {
            Map<String, Object> consumerConfigs = Map.of(GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT));
            sendRecords(100, topicPartition1, System.currentTimeMillis());
            sendRecords(100, topicPartition2, System.currentTimeMillis());
            String topic3 = "TOPIC_3";
            Set<TopicPartition> subscriptions = createTopicAndSendRecords(topic3, 6, 100);
            subscriptions.addAll(Set.of(topicPartition1, topicPartition2));

            // first subscribe consumers that are defined in this class
            consumerPollers.add(subscribeConsumerAndStartPolling(cluster.consumer(consumerConfigs), List.of(TOPIC_1, topic3), Set.of()));
            consumerPollers.add(subscribeConsumerAndStartPolling(cluster.consumer(consumerConfigs), List.of(TOPIC_1, topic3), Set.of()));

            // create one more consumer and add it to the group; we will timeout this consumer
            try (Consumer<byte[], byte[]> timeoutConsumer = cluster.consumer(consumerConfigs)) {
                ConsumerAssignmentPoller timeoutPoller = subscribeConsumerAndStartPolling(timeoutConsumer, List.of(TOPIC_1, topic3), Set.of());
                consumerPollers.add(timeoutPoller);

                // validate the initial assignment
                validateGroupAssignment(consumerPollers, subscriptions, Optional.empty(), 10000L, List.of());

                // stop polling and close one of the consumers, should trigger partition re-assignment among alive consumers
                timeoutPoller.shutdown();
                consumerPollers.remove(timeoutPoller);
                if (closeConsumer)
                    timeoutConsumer.close();

                validateGroupAssignment(consumerPollers,
                    subscriptions,
                    Optional.of("Did not get valid assignment for partitions " + subscriptions + " after one consumer left"),
                    180000,
                    List.of());
            }
        } finally {
            for (ConsumerAssignmentPoller poller : consumerPollers) {
                poller.shutdown();
            }
        }
    }

    @ClusterTest
    void testClassicConsumerPollEventuallyReturnsRecordsWithZeroTimeout() throws InterruptedException {
        testPollEventuallyReturnsRecordsWithZeroTimeout(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    void testAsyncConsumerPollEventuallyReturnsRecordsWithZeroTimeout() throws InterruptedException {
        testPollEventuallyReturnsRecordsWithZeroTimeout(GroupProtocol.CONSUMER);
    }

    private void testPollEventuallyReturnsRecordsWithZeroTimeout(GroupProtocol groupProtocol) throws InterruptedException {
        int numberOfRecords = 100;
        sendRecords(numberOfRecords, topicPartition1, System.currentTimeMillis());
        try (Consumer<byte[], byte[]> consumer = cluster.consumer(Map.of(GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT)))) {
            consumer.subscribe(Set.of(TOPIC_1));
            AtomicInteger totalRecords = new AtomicInteger();
            TestUtils.waitForCondition(() -> {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
                totalRecords.addAndGet(records.count());
                return !records.records(topicPartition1).isEmpty();
            }, 15000, "Consumer did not consume any messages for partition " + topicPartition1 + " before timeout.", OptionalLong.of(0));
            assertEquals(numberOfRecords, totalRecords.get());
        }
    }

    @ClusterTest
    void testClassicConsumerNoOffsetForPartitionExceptionOnPollZero() throws InterruptedException {
        testNoOffsetForPartitionExceptionOnPollZero(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    void testAsyncConsumerNoOffsetForPartitionExceptionOnPollZero() throws InterruptedException {
        testNoOffsetForPartitionExceptionOnPollZero(GroupProtocol.CONSUMER);
    }

    private void testNoOffsetForPartitionExceptionOnPollZero(GroupProtocol groupProtocol) throws InterruptedException {
        Map<String, Object> consumerConfigs = Map.of(
            GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT),
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none"
        );
        try (Consumer<byte[], byte[]> consumer = cluster.consumer(consumerConfigs)) {
            consumer.assign(List.of(topicPartition1));
            TestUtils.waitForCondition(() -> {
                assertThrows(NoOffsetForPartitionException.class, () -> consumer.poll(Duration.ZERO));
                return true;
            }, "continuous poll should eventually fail because there is no offset reset strategy set (fail only when resetting positions after coordinator is known)");
        }
    }

    private void sendRecords(int numRecords, TopicPartition topicPartition, long startingTimestamp) {
        try (Producer<byte[], byte[]> producer = cluster.producer()) {
            for (int i = 0; i < numRecords; i++) {
                long timestamp = startingTimestamp + i;
                var record = new ProducerRecord<>(
                    topicPartition.topic(),
                    topicPartition.partition(),
                    timestamp,
                    ("key " + i).getBytes(),
                    ("value " + i).getBytes()
                );
                producer.send(record);
            }
            producer.flush();
        }
    }

    private void consumeAndVerifyRecords(Consumer<byte[], byte[]> consumer,
                                         int numberOfRecords,
                                         int startingOffset,
                                         int startingKeyAndValueIndex,
                                         long startingTimestamp,
                                         TimestampType timestampType,
                                         TopicPartition topicPartition,
                                         int maxPollRecords) throws InterruptedException {
        List<ConsumerRecord<byte[], byte[]>> records = consumerRecords(consumer, numberOfRecords, maxPollRecords);
        long now = System.currentTimeMillis();
        for (int i = 0; i < numberOfRecords; i++) {
            ConsumerRecord<byte[], byte[]> record = records.get(i);
            int offset = startingOffset + i;
            assertEquals(topicPartition.topic(), record.topic());
            assertEquals(topicPartition.partition(), record.partition());
            if (TimestampType.CREATE_TIME == timestampType) {
                assertEquals(timestampType, record.timestampType());
                assertEquals(startingTimestamp + i, record.timestamp());
            } else {
                assertTrue(record.timestamp() >= startingTimestamp && record.timestamp() <= now,
                    "Got unexpected timestamp " + record.timestamp() + ". Timestamp should be between [" + startingTimestamp + ", " + now + "]");
                assertEquals(offset, record.offset());
                int keyAndValueIndex = startingKeyAndValueIndex + i;
                assertEquals("key" + keyAndValueIndex, new String(record.key()));
                assertEquals("value" + keyAndValueIndex, new String(record.value()));
                // this is true only because K and V are byte arrays
                assertEquals(("key" + keyAndValueIndex).length(), record.serializedKeySize());
                assertEquals(("value" + keyAndValueIndex).length(), record.serializedValueSize());
            }
        }

    }

    private List<ConsumerRecord<byte[], byte[]>> consumerRecords(Consumer<byte[], byte[]> consumer, int numberOfRecords, int maxPollRecords) throws InterruptedException {
        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        TestUtils.waitForCondition(() -> {
            ConsumerRecords<byte[], byte[]> pollRecords = consumer.poll(Duration.ofMillis(100));
            assertTrue(pollRecords.count() <= maxPollRecords);
            pollRecords.forEach(records::add);
            return records.size() >= numberOfRecords;
        }, 60000, "Timed out before consuming expected " + numberOfRecords + " records.", OptionalLong.of(0));
        return records;
    }

    private void awaitRebalance(Consumer<byte[], byte[]> consumer, TestConsumerReassignmentListener rebalanceListener) throws InterruptedException {
        int numberOfReassignments = rebalanceListener.callsToAssigned();
        TestUtils.waitForCondition(() -> {
            consumer.poll(Duration.ofMillis(100));
            return rebalanceListener.callsToAssigned() > numberOfReassignments;
        }, "Timed out before expected rebalance completed");
    }

    private void ensureNoRebalance(Consumer<byte[], byte[]> consumer, TestConsumerReassignmentListener rebalanceListener) throws InterruptedException {
        // The best way to verify that the current membership is still active is to commit offsets.
        // This would fail if the group had rebalanced.
        int initialRevokeCalls = rebalanceListener.callsToRevoked();
        sendAndAwaitAsyncCommit(consumer, Optional.empty());
        assertEquals(rebalanceListener.callsToRevoked(), initialRevokeCalls);
    }

    private void sendAndAwaitAsyncCommit(Consumer<byte[], byte[]> consumer, Optional<Map<TopicPartition, OffsetAndMetadata>> offsets) throws InterruptedException {
        java.util.function.Consumer<OffsetCommitCallback> sendAsyncCommit = callback -> offsets.ifPresentOrElse(
            o -> consumer.commitAsync(o, callback), () -> consumer.commitAsync(callback));
        RetryCommitCallback commitCallback = new RetryCommitCallback(sendAsyncCommit);
        sendAsyncCommit.accept(commitCallback);
        TestUtils.waitForCondition(() -> {
            consumer.poll(Duration.ofMillis(100));
            return commitCallback.completed();
        }, 10000, "Failed to observe commit callback before timeout", OptionalLong.of(0));
        assertTrue(commitCallback.error().isEmpty());
    }

    private Set<TopicPartition> createTopicAndSendRecords(String topic, int numberOfPartitions, int recordsPerPartition) throws ExecutionException, InterruptedException {
        try (Admin admin = cluster.admin()) {
            admin.createTopics(List.of(new NewTopic(topic, numberOfPartitions, (short) 3))).all().get();
            Set<TopicPartition> topicPartitions = new HashSet<>();
            for (int i = 0; i < numberOfPartitions; i++) {
                TopicPartition topicPartition = new TopicPartition(topic, i);
                sendRecords(recordsPerPartition, topicPartition, System.currentTimeMillis());
                topicPartitions.add(topicPartition);
            }
            return topicPartitions;
        }
    }

    private ConsumerAssignmentPoller subscribeConsumerAndStartPolling(
        Consumer<byte[], byte[]> consumer,
        List<String> topicsToSubscribe,
        Set<TopicPartition> partitionToAssign
    ) {
        assertTrue(consumer.assignment().isEmpty());
        ConsumerAssignmentPoller consumerPoller = !topicsToSubscribe.isEmpty() ?
            new ConsumerAssignmentPoller(consumer, topicsToSubscribe) : new ConsumerAssignmentPoller(consumer, partitionToAssign);
        consumerPoller.start();
        return consumerPoller;
    }

    private void validateGroupAssignment(
        List<ConsumerAssignmentPoller> consumerPollers,
        Set<TopicPartition> subscriptions,
        Optional<String> message,
        long waitTimeMs,
        List<Set<TopicPartition>> expectedAssignment
    ) throws InterruptedException {
        List<Set<TopicPartition>> assignments = new ArrayList<>();
        TestUtils.waitForCondition(() -> {
            assignments.clear();
            consumerPollers.forEach(poller -> assignments.add(poller.consumerAssignment()));
            return isPartitionAssignmentValid(assignments, subscriptions, expectedAssignment);
        }, waitTimeMs, message.orElse("Did not get valid assignment for partitions " + subscriptions + ". Instead, got $assignments"));
    }

    private boolean isPartitionAssignmentValid(
        List<Set<TopicPartition>> assignments,
        Set<TopicPartition> partitions,
        List<Set<TopicPartition>> expectedAssignments
    ) {
        boolean allNonEmptyAssignments = assignments.stream().noneMatch(Set::isEmpty);
        if (!allNonEmptyAssignments) {
            // at least one consumer got empty assignment
            System.err.println("all");
            return false;
        }

        // make sure that sum of all partitions to all consumers equals total number of partitions
        int totalPartitionsInAssignments = assignments.stream().mapToInt(Set::size).sum();
        if (totalPartitionsInAssignments != partitions.size()) {
            // either same partitions got assigned to more than one consumer or some
            // partitions were not assigned
            System.err.println("size");
            return false;
        }


        // The above checks could miss the case where one or more partitions were assigned to more
        // than one consumer and the same number of partitions were missing from assignments.
        // Make sure that all unique assignments are the same as 'partitions'
        var uniqueAssignedPartitions = assignments.stream()
            .flatMap(Set::stream)
            .collect(Collectors.toSet());
        if (!uniqueAssignedPartitions.equals(partitions)) {
            System.err.println("equals");
            return false;
        }

        // check the assignment is the same as the expected assignment if provided
        // Note: since we've checked that each partition is assigned to only one consumer,
        // we just need to check the assignment is included in the expected assignment
        if (!expectedAssignments.isEmpty()) {
            for (Set<TopicPartition> assignment : assignments) {
                if (!expectedAssignments.contains(assignment)) {
                    System.err.println("contains");
                    return false;
                }
            }
        }
        return true;
    }

    static class TestConsumerReassignmentListener implements ConsumerRebalanceListener {
        private int callsToAssigned = 0;
        private int callsToRevoked = 0;

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            callsToRevoked += 1;
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            callsToAssigned += 1;
        }

        public int callsToAssigned() {
            return callsToAssigned;
        }

        public int callsToRevoked() {
            return callsToRevoked;
        }
    }

    static class TestDelayInRevocationConsumerReassignmentListener extends TestConsumerReassignmentListener {
        private final Consumer<byte[], byte[]> consumer;
        private final TopicPartition tp;
        private boolean commitCompleted = false;
        private long committedPosition = -1;

        public TestDelayInRevocationConsumerReassignmentListener(Consumer<byte[], byte[]> consumer, TopicPartition topicPartition) {
            this.consumer = consumer;
            tp = topicPartition;
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions) {
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            if (!partitions.isEmpty() && partitions.contains(tp)) {
                // on the second rebalance (after we have joined the group initially), sleep longer
                // than session timeout and then try a commit. We should still be in the group,
                // so the commit should succeed
                Utils.sleep(1500);
                committedPosition = consumer.position(tp);
                consumer.commitSync(Map.of(tp, new OffsetAndMetadata(committedPosition)));
                commitCompleted = true;
            }
            super.onPartitionsRevoked(partitions);
        }

        public boolean commitCompleted() {
            return commitCompleted;
        }

        public long committedPosition() {
            return committedPosition;
        }
    }

    static class RetryCommitCallback implements OffsetCommitCallback {
        private final java.util.function.Consumer<OffsetCommitCallback> callback;
        private boolean isCompleted = false;
        private Optional<Exception> error = Optional.empty();

        public RetryCommitCallback(java.util.function.Consumer<OffsetCommitCallback> callback) {
            this.callback = callback;
        }

        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            if (exception instanceof RetriableCommitFailedException)
                this.callback.accept(this);
            else {
                isCompleted = true;
                error = Optional.ofNullable(exception);
            }
        }

        public boolean completed() {
            return isCompleted;
        }

        public Optional<Exception> error() {
            return error;
        }
    }
}
