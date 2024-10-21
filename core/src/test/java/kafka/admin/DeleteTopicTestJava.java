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
package kafka.admin;

import kafka.log.UnifiedLog;
import kafka.server.KafkaBroker;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataPartitionState;
import org.apache.kafka.common.test.TestUtils;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.ClusterTestExtensions;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.metadata.BrokerState;
import org.apache.kafka.storage.internals.checkpoint.OffsetCheckpointFile;
import org.junit.jupiter.api.extension.ExtendWith;
import scala.jdk.javaapi.OptionConverters;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;


@ExtendWith(value = ClusterTestExtensions.class)
@ClusterTestDefaults(types = {Type.KRAFT}, serverProperties = {
        @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
        @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
})
public class DeleteTopicTestJava {
    private static final String DEFAULT_TOPIC = "topic";
    private final Map<Integer, List<Integer>> expectedReplicaAssignment = Map.of(0, List.of(0, 1, 2));

    @ClusterTest
    public void testDeleteTopicWithAllAliveReplicas(ClusterInstance cluster) throws Exception {
        cluster.createTopic(DEFAULT_TOPIC, 1, (short) 1);
        try (Admin admin = cluster.createAdminClient()) {
            admin.deleteTopics(List.of(DEFAULT_TOPIC)).all().get();
            waitUtilTopicDelete(admin);
        }
    }

    @ClusterTest(brokers = 3, serverProperties = {@ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "3")})
    public void testResumeDeleteTopicWithRecoveredFollower(ClusterInstance cluster) throws Exception {
        cluster.createTopic(DEFAULT_TOPIC, 1, (short) 3);
        TopicPartition topicPartition = new TopicPartition(DEFAULT_TOPIC, 0);

        try (Admin admin = cluster.createAdminClient()) {
            Map<Integer, KafkaBroker> idToBroker = cluster.brokers();
            int leaderId = waitUtilLeaderIsKnown(idToBroker, topicPartition).get();
            KafkaBroker follower = findFollower(idToBroker, leaderId);

            follower.shutdown();
            admin.deleteTopics(List.of(DEFAULT_TOPIC)).all().get();

            idToBroker.values()
                    .stream()
                    .filter(broker -> broker.config().brokerId() != follower.config().brokerId())
                    .forEach(b -> {
                        try {
                            TestUtils.waitForCondition(() -> b.logManager().getLog(topicPartition, false).isEmpty(),
                                    "Replicas 0,1 have not deleted log.");
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    });

            follower.startup();
            waitUtilTopicDelete(admin);
        }
    }

    @ClusterTest(brokers = 4, serverProperties = {@ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "3")})
    public void testPartitionReassignmentDuringDeleteTopic(ClusterInstance cluster) throws Exception {
        cluster.createTopic(DEFAULT_TOPIC, 1, (short) 4);
        TopicPartition topicPartition = new TopicPartition(DEFAULT_TOPIC, 0);
        try (Admin admin = cluster.createAdminClient()) {
            Map<Integer, KafkaBroker> brokers = cluster.brokers();
            Map<Integer, KafkaBroker> servers = findPartitionHostingBrokers(brokers);
            int leaderId = waitUtilLeaderIsKnown(brokers, topicPartition).get();
            KafkaBroker follower = findFollower(servers, leaderId);
            follower.shutdown();

            admin.deleteTopics(List.of(DEFAULT_TOPIC)).all().get();
            Properties properties = new Properties();
            properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());

            try (Admin otherAdmin = Admin.create(properties)) {
                waitUtilTopicGone(otherAdmin);
                assertThrows(ExecutionException.class, () -> otherAdmin.alterPartitionReassignments(
                        Map.of(topicPartition, Optional.of(new NewPartitionReassignment(List.of(1, 2, 3))))
                ).all().get());
            }

            follower.startup();
            waitUtilTopicDelete(admin);
        }
    }

    @ClusterTest(brokers = 4, serverProperties = {@ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "3")})
    public void testIncreasePartitionCountDuringDeleteTopic(ClusterInstance cluster) throws Exception {
        cluster.createTopic(DEFAULT_TOPIC, 1, (short) 4);
        TopicPartition topicPartition = new TopicPartition(DEFAULT_TOPIC, 0);
        try (Admin admin = cluster.createAdminClient()) {
            Map<Integer, KafkaBroker> partitionHostingBrokers = findPartitionHostingBrokers(cluster.brokers());
            int leaderId = waitUtilLeaderIsKnown(partitionHostingBrokers, topicPartition).get();
            KafkaBroker follower = findFollower(partitionHostingBrokers, leaderId);
            follower.shutdown();
            admin.deleteTopics(List.of(DEFAULT_TOPIC)).all().get();

            Properties properties = new Properties();
            properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());

            try (Admin otherAdmin = Admin.create(properties)) {
                otherAdmin.createPartitions(Map.of(DEFAULT_TOPIC, NewPartitions.increaseTo(2))).all().get();
            } catch (ExecutionException exception) {

            }

            follower.startup();
            // TODO verify ??
            waitUtilTopicDelete(admin);
        }
    }

    @ClusterTest
    public void testDeleteTopicDuringAddPartition(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.createAdminClient()) {
            cluster.createTopic(DEFAULT_TOPIC, 1, (short) 1);
            int leaderId = waitUtilLeaderIsKnown(cluster.brokers(), new TopicPartition(DEFAULT_TOPIC, 0)).get();
            TopicPartition topicPartition = new TopicPartition(DEFAULT_TOPIC, 0);
            KafkaBroker follower = findFollower(cluster.brokers(), leaderId);
            follower.shutdown();
            TestUtils.waitForCondition(() -> follower.brokerState().equals(BrokerState.SHUTTING_DOWN),
                    "Follower " + follower.config().brokerId() + " was not shutdown");
            increasePartitions(admin, DEFAULT_TOPIC, 3, Collections.emptyList());
            admin.deleteTopics(List.of(DEFAULT_TOPIC)).all().get();
            follower.startup();
            verifyTopicDeletion(DEFAULT_TOPIC, 1, cluster.brokers().values());
            TestUtils.waitForCondition(() -> cluster.brokers().values()
                        .stream().allMatch(broker -> broker.logManager().getLog(topicPartition, false).isEmpty()),
                    "Replica logs not for new partition [" + DEFAULT_TOPIC + ",1] not deleted after delete topic is complete.");
        }
    }

    private Optional<Integer> waitUtilLeaderIsKnown(Map<Integer, KafkaBroker> idToBroker,
                                                    TopicPartition topicPartition) throws InterruptedException {
        Optional<Integer> brokerId = idToBroker.values()
                .stream()
                .filter(broker -> broker.replicaManager()
                        .onlinePartition(topicPartition)
                        .exists(tp -> tp.leaderIdIfLocal().isDefined()))
                .map(broker -> broker.config().brokerId())
                .findFirst();
        TestUtils.waitForCondition(brokerId::isPresent,
                "Partition " + topicPartition + " not made yet" + " after 15 seconds");
        return brokerId;
    }

    private KafkaBroker findFollower(Map<Integer, KafkaBroker> idToBroker, int leaderId) {
        return idToBroker.entrySet()
                .stream()
                .filter(entry -> entry.getKey().equals(leaderId))
                .findFirst()
                .map(Map.Entry::getValue)
                .orElseGet(() -> fail("Can't find any follower"));
    }

    private void waitUtilTopicDelete(Admin admin) throws InterruptedException {
        TestUtils.waitForCondition(() -> {
            try {
                return admin.listTopics().listings().get().isEmpty();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, "Topics should be deleted");
    }

    private void waitUtilTopicGone(Admin admin) throws Exception {
        TestUtils.waitForCondition(() -> {
            try {
                admin.describeTopics(List.of(DEFAULT_TOPIC)).allTopicNames().get();
                return false;
            } catch (ExecutionException exception) {
                return exception.getCause().getClass().equals(UnknownTopicOrPartitionException.class);
            } catch (InterruptedException e) {
                return false;
            }
        }, "Topic" + DEFAULT_TOPIC + " should be deleted");
    }

    private Map<Integer, KafkaBroker> findPartitionHostingBrokers(Map<Integer, KafkaBroker> brokers) {
        return brokers.entrySet()
                .stream()
                .filter(b -> expectedReplicaAssignment.get(0).contains(b.getValue().config().brokerId()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
    public <B extends KafkaBroker> void increasePartitions(Admin admin,
                                                           String topic,
                                                           int totalPartitionCount,
                                                           List<B> brokersToValidate) throws Exception {
        Map<String, NewPartitions> newPartitionSet = Map.of(topic, NewPartitions.increaseTo(totalPartitionCount));
        admin.createPartitions(newPartitionSet);

        if (!brokersToValidate.isEmpty()) {
            // wait until we've propagated all partitions metadata to all brokers
            Map<TopicPartition, UpdateMetadataPartitionState> allPartitionsMetadata = waitForAllPartitionsMetadata(brokersToValidate, topic, totalPartitionCount);

            IntStream.range(0, totalPartitionCount - 1).forEach(i -> {
                Optional<UpdateMetadataPartitionState> partitionMetadata = Optional.ofNullable(allPartitionsMetadata.get(new TopicPartition(topic, i)));
                partitionMetadata.ifPresent(metadata -> assertEquals(totalPartitionCount, metadata.replicas().size()));
            });
        }
    }
    public <B extends KafkaBroker> Map<TopicPartition, UpdateMetadataPartitionState> waitForAllPartitionsMetadata(
            List<B> brokers, String topic, int expectedNumPartitions) throws Exception {

        // Wait until all brokers have the expected partition metadata
        TestUtils.waitForCondition(() -> brokers.stream().allMatch(broker -> {
            if (expectedNumPartitions == 0) {
                return broker.metadataCache().numPartitions(topic).isEmpty();
            } else {
                return OptionConverters.toJava(broker.metadataCache().numPartitions(topic))
                        .equals(Optional.of(expectedNumPartitions));
            }
        }), 60000, "Topic [" + topic + "] metadata not propagated after 60000 ms");

        // Since the metadata is propagated, we should get the same metadata from each server
        Map<TopicPartition, UpdateMetadataPartitionState> partitionMetadataMap = new HashMap<>();
        IntStream.range(0, expectedNumPartitions).forEach(i -> {
            TopicPartition topicPartition = new TopicPartition(topic, i);
            UpdateMetadataPartitionState partitionState = OptionConverters.toJava(brokers.get(0).metadataCache()
                    .getPartitionInfo(topic, i)).orElseThrow(() ->
                            new IllegalStateException("Cannot get topic: " + topic + ", partition: " + i + " in server metadata cache"));
            partitionMetadataMap.put(topicPartition, partitionState);
        });

        return partitionMetadataMap;
    }
    public <B extends KafkaBroker> void verifyTopicDeletion(String topic,
                                                            int numPartitions,
                                                            Collection<B> brokers) throws Exception {
        List<TopicPartition> topicPartitions = IntStream.range(0, numPartitions)
                .mapToObj(partition -> new TopicPartition(topic, partition))
                .collect(Collectors.toList());

        // Ensure that the topic-partition has been deleted from all brokers' replica managers
        TestUtils.waitForCondition(() -> brokers.stream().allMatch(broker ->
                        topicPartitions.stream().allMatch(tp -> broker.replicaManager().onlinePartition(tp).isEmpty())),
                "Replica manager's should have deleted all of this topic's partitions");

        // Ensure that logs from all replicas are deleted
        TestUtils.waitForCondition(() -> brokers.stream().allMatch(broker ->
                        topicPartitions.stream().allMatch(tp -> broker.logManager().getLog(tp, false).isEmpty())),
                "Replica logs not deleted after delete topic is complete");

        // Ensure that the topic is removed from all cleaner offsets
        TestUtils.waitForCondition(() -> brokers.stream().allMatch(broker ->
                        topicPartitions.stream().allMatch(tp -> broker.logManager().liveLogDirs().forall(logDir -> {
                            OffsetCheckpointFile checkpointFile;
                            try {
                                checkpointFile = new OffsetCheckpointFile(new File(logDir, "cleaner-offset-checkpoint"), null);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                            return !checkpointFile.read().containsKey(tp);
                        }))),
                "Cleaner offset for deleted partition should have been removed");

        // Ensure that the topic directories are soft-deleted
        TestUtils.waitForCondition(() -> brokers.stream().allMatch(broker ->
                        broker.config().logDirs().forall(logDir ->
                                topicPartitions.stream().noneMatch(tp ->
                                        new File(logDir, tp.topic() + "-" + tp.partition()).exists()))),
                "Failed to soft-delete the data to a delete directory");

        // Ensure that the topic directories are hard-deleted
        TestUtils.waitForCondition(() -> brokers.stream().allMatch(broker ->
                        broker.config().logDirs().forall(logDir ->
                                topicPartitions.stream().allMatch(tp ->
                                        Arrays.stream(new File(logDir).list())
                                                .noneMatch(partitionDirectoryName ->
                                                        partitionDirectoryName.startsWith(tp.topic() + "-" + tp.partition()) &&
                                                        partitionDirectoryName.endsWith(UnifiedLog.DeleteDirSuffix()))))),
                "Failed to hard-delete the delete directory");
    }
}
