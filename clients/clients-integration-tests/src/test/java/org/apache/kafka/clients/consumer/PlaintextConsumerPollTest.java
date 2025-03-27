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


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.TestUtils;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.IntStream;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for the consumer that covers the poll logic
 */
@Timeout(600)
@ClusterTestDefaults(
    serverProperties = {
        @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "3"),
        @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, value = "100"),
        @ClusterConfigProperty(key = GroupCoordinatorConfig.GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG, value = "60000"),
        @ClusterConfigProperty(key = GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, value = "10"),
        @ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_CONFIG, value = "500"),
        @ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG, value = "500"),
    }
)
public class PlaintextConsumerPollTest {

    @ClusterTest
    void testMaxPollRecords(ClusterInstance cluster) {

    }

    private Producer<byte[], byte[]> createProducer(String bootstrapServer) {
        Map<String, Object> producerConfig = Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer,
            ProducerConfig.ACKS_CONFIG, "-1"
        );
        return new KafkaProducer<>(producerConfig, new ByteArraySerializer(), new ByteArraySerializer());
    }

    private Consumer<byte[], byte[]> createConsumer(GroupProtocol protocol, String bootstrapServer) {
        Map<String, Object> consumerConfig = Map.of(
            CLIENT_ID_CONFIG, "ConsumerTestConsumer",
            GROUP_ID_CONFIG, "TestGroup",
            GROUP_PROTOCOL_CONFIG, protocol.name().toLowerCase(Locale.ROOT),
            BOOTSTRAP_SERVERS_CONFIG, bootstrapServer
        );
        return new KafkaConsumer<>(consumerConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    private List<ProducerRecord<byte[], byte[]>> sendRecords(Producer<byte[], byte[]> producer,
                                                             int numberOfRecords,
                                                             TopicPartition topicPartition,
                                                             long startTimestamp,
                                                             long timestampIncrement) {
        return IntStream.range(0, numberOfRecords)
            .mapToObj(i -> produceRecords(producer, topicPartition, startTimestamp, timestampIncrement, i))
            .toList();
    }

    private ProducerRecord<byte[], byte[]> produceRecords(Producer<byte[], byte[]> producer,
                                                          TopicPartition topicPartition,
                                                          long startingTimestamp,
                                                          long timestampIncrement,
                                                          int i) {
        long timestamp = timestampIncrement > 0 ? (startingTimestamp + timestampIncrement * i) : (startingTimestamp + i);
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topicPartition.topic(),
            topicPartition.partition(),
            timestamp,
            ("key" + i).getBytes(),
            ("value" + i).getBytes());
        producer.send(record);
        return record;
    }

    private void consumeAndVerifyRecords(Consumer<byte[], byte[]> consumer,
                                         int numberOfRecords,
                                         int startingOffset,
                                         int startingKeyAndValueIndex,
                                         int startingTimestamp,
                                         TimestampType timestampType,
                                         TopicPartition topicPartition,
                                         int maxPollRecords,
                                         long timestampIncrement) throws InterruptedException {
        List<ConsumerRecord<byte[], byte[]>> records = consumerRecords(consumer, numberOfRecords, maxPollRecords);
        long now = System.currentTimeMillis();
        for (int i = 0; i < numberOfRecords; i++) {
            ConsumerRecord<byte[], byte[]> record = records.get(i);
            int offset = startingOffset + i;
            assertEquals(topicPartition.topic(), record.topic());
            assertEquals(topicPartition.partition(), record.partition());
            if (TimestampType.CREATE_TIME == timestampType) {
                assertEquals(timestampType, record.timestampType());
                if (timestampIncrement > 0)
                    assertEquals(startingTimestamp + i * timestampIncrement, record.timestamp());
                else
                    assertEquals(startingTimestamp + i, record.timestamp());
            } else {
                assertTrue(record.timestamp() >= startingTimestamp && record.timestamp() <= now,
                    "Got unexpected timestamp " + record.timestamp() + ". Timestamp should be between [" + startingTimestamp + ", " + now +"]");
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
            return records.size() >= numberOfRecords;
        }, "");
        return records;
    }
}
