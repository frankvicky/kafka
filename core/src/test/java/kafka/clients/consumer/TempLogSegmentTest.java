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
package kafka.clients.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.ClusterTestExtensions;
import org.apache.kafka.common.test.api.Type;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;


@ClusterTestDefaults(
    types = {Type.KRAFT},
    serverProperties = {
        @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
        @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
        @ClusterConfigProperty(key = "group.coordinator.new.enable", value = "false"),
        @ClusterConfigProperty(key = "message.max.bytes", value = "10485760"),
        @ClusterConfigProperty(key = "replica.fetch.max.bytes", value = "10485760"),
    }
)
@ExtendWith(ClusterTestExtensions.class)
public class TempLogSegmentTest {
    private final String topic = "topic";

    @ClusterTest
    public void temp(ClusterInstance clusterInstance) throws Exception {
        Map<String, Object> producerConfigs = Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers(),
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
            ProducerConfig.BATCH_SIZE_CONFIG, 10240 * 1024,
            ProducerConfig.LINGER_MS_CONFIG, 10000,
            ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10240 * 1024
        );

        StringBuilder messageValue = new StringBuilder();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfigs)) {
            for (int i = 0; i < 400000; i++) {
                // "This is a message" is 26 byes.
                messageValue.append("This is a message").append(i).append("\n");
            }

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, messageValue.toString());

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    exception.printStackTrace();
                } else {
                    System.out.println("Message sent to partition " + metadata.partition() + ", offset " + metadata.offset());
                }
            });

            producer.flush();
        }

        Map<String, Object> consumerConfigs = Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers(),
            ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString(),
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()
        );

        try (Consumer<String, String> consumer = new KafkaConsumer<>(consumerConfigs)) {
            TopicPartition partition = new TopicPartition(topic, 0);
            consumer.assign(List.of(partition));
            long start = System.currentTimeMillis();
            consumer.seek(partition, 300000);
            System.err.println(System.currentTimeMillis() - start);
        }
    }

    @ClusterTest
    public void temp1(ClusterInstance clusterInstance) throws Exception {
        Map<String, Object> producerConfigs = Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers(),
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
            ProducerConfig.BATCH_SIZE_CONFIG, 1024 * 1024,
            ProducerConfig.LINGER_MS_CONFIG, 100,
            ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10240 * 1024
        );

        StringBuilder messageValue = new StringBuilder();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfigs)) {
            for (int i = 0; i < 40000; i++) {
                // "This is a message" is 26 byes.
                messageValue.append("This is a message").append(i).append("\n");
            }

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, messageValue.toString());

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    exception.printStackTrace();
                } else {
                    System.out.println("Message sent to partition " + metadata.partition() + ", offset " + metadata.offset());
                }
            });

            producer.flush();
        }

        Map<String, Object> consumerConfigs = Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers(),
            ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString(),
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()
        );

        try (Consumer<String, String> consumer = new KafkaConsumer<>(consumerConfigs)) {
            TopicPartition partition = new TopicPartition(topic, 0);
            consumer.assign(List.of(partition));
            long start = System.currentTimeMillis();
            consumer.seek(partition, 30000);
            System.err.println(System.currentTimeMillis() - start);
        }
    }
}
