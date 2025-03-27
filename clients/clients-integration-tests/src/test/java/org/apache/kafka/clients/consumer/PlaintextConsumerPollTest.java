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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.TestUtils;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
    private static final String TOPIC = "TOPIC";
    private static final int PARTITION = 0;
    private final TopicPartition topicPartition = new TopicPartition(TOPIC, PARTITION);
    private final ClusterInstance cluster;

    public PlaintextConsumerPollTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @BeforeEach
    void setUp() {
        try (Admin admin = cluster.admin()) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 2, (short) 3)));
        }
    }

    @ClusterTest
    void testMaxPollRecords() throws InterruptedException {
        try (Producer<byte[], byte[]> producer = createProducer(cluster)) {
            int maxPollRecords = 2;
            int numberOfRecords = 10000;
            long startingTimestamp = System.currentTimeMillis();
            sendRecords(producer, numberOfRecords, topicPartition, startingTimestamp);
            Map<String, Object> consumerConfigs = Map.of(
                GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT),
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords
            );
            try (Consumer<byte[], byte[]> consumer = cluster.consumer(consumerConfigs)) {
                consumer.assign(List.of(topicPartition));
                consumeAndVerifyRecords(consumer,
                    numberOfRecords,
                    0,
                    0,
                    startingTimestamp,
                    TimestampType.CREATE_TIME,
                    topicPartition,
                    maxPollRecords);
            }

        }
    }

    private Producer<byte[], byte[]> createProducer(ClusterInstance cluster) {
        return cluster.producer(Map.of(ProducerConfig.ACKS_CONFIG, "-1"));
    }

    private List<ProducerRecord<byte[], byte[]>> sendRecords(Producer<byte[], byte[]> producer,
                                                             int numberOfRecords,
                                                             TopicPartition topicPartition,
                                                             long startTimestamp) {
        List<ProducerRecord<byte[], byte[]>> list = new ArrayList<>();
        for (int i = 0; i < numberOfRecords; i++) {
            long timestamp = startTimestamp + i;
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topicPartition.topic(),
                topicPartition.partition(),
                timestamp,
                ("key" + i).getBytes(),
                ("value" + i).getBytes());
            producer.send(record);
            list.add(record);
        }
        producer.flush();
        return list;
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
        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>(numberOfRecords);
        TestUtils.waitForCondition(() -> {
            ConsumerRecords<byte[], byte[]> pollRecords = consumer.poll(Duration.ofMillis(100));
            assertTrue(pollRecords.count() <= maxPollRecords);
            pollRecords.forEach(records::add);
            System.err.println(records.size());
            return records.size() >= numberOfRecords;
        }, 60000, "Timed out before consuming expected " + numberOfRecords + " records.");
        return records;
    }
}
