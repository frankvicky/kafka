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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.server.util.ShutdownableThread;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ConsumerAssignmentPoller extends ShutdownableThread {
    private final Consumer<byte[], byte[]> consumer;
    private Optional<Throwable> thrownException = Optional.empty();
    private final AtomicInteger receivedMessages = new AtomicInteger(0);
    private final Set<TopicPartition> partitionAssignment = new HashSet<>();
    private final AtomicBoolean subscriptionChanged = new AtomicBoolean(false);
    private volatile List<String> topicToSubscribe;
    private final Set<TopicPartition> partitionsToAssign;
    private final ConsumerRebalanceListener userRebalanceListener;

    public ConsumerAssignmentPoller(Consumer<byte[], byte[]> consumer,
                                    List<String> topicsToSubscribe,
                                    Set<TopicPartition> partitionsToAssign,
                                    ConsumerRebalanceListener userRebalanceListener) {
        super("daemon-consumer-assignment", false);
        this.consumer = consumer;
        this.topicToSubscribe = topicsToSubscribe;
        this.partitionsToAssign = partitionsToAssign;
        this.userRebalanceListener = userRebalanceListener;

        if (partitionsToAssign.isEmpty()) {
            consumer.subscribe(new ArrayList<>(topicsToSubscribe), rebalanceListener);
        } else {
            consumer.assign(new ArrayList<>(partitionsToAssign));
        }
    }

    public ConsumerAssignmentPoller(Consumer<byte[], byte[]> consumer, List<String> topicsToSubscribe) {
        this(consumer, topicsToSubscribe, Collections.emptySet(), null);
    }

    public ConsumerAssignmentPoller(Consumer<byte[], byte[]> consumer, Set<TopicPartition> partitionsToAssign) {
        this(consumer, Collections.emptyList(), partitionsToAssign, null);
    }

    private final ConsumerRebalanceListener rebalanceListener = new ConsumerRebalanceListener() {
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            partitionAssignment.addAll(partitions);
            if (userRebalanceListener != null) {
                userRebalanceListener.onPartitionsAssigned(partitions);
            }
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            partitionAssignment.removeAll(partitions);
            if (userRebalanceListener != null) {
                userRebalanceListener.onPartitionsRevoked(partitions);
            }
        }
    };

    public Optional<Throwable> getThrownException() {
        return thrownException;
    }

    public int getReceivedMessages() {
        return receivedMessages.get();
    }

    public Set<TopicPartition> consumerAssignment() {
        return Collections.unmodifiableSet(partitionAssignment);
    }

    /**
     * Subscribe consumer to a new set of topics.
     * Since this method most likely be called from a different thread, this function
     * just "schedules" the subscription change, and actual call to consumer.subscribe is done
     * in the doWork() method
     *
     * This method does not allow to change subscription until doWork processes the previous call
     * to this method. This is just to avoid race conditions and enough functionality for testing purposes
     * @param newTopicsToSubscribe new topics to subscribe to
     */
    public void subscribe(List<String> newTopicsToSubscribe) {
        if (subscriptionChanged.get()) {
            throw new IllegalStateException("Do not call subscribe until the previous subscribe request is processed.");
        }
        if (!partitionsToAssign.isEmpty()) {
            throw new IllegalStateException("Cannot call subscribe when configured to use manual partition assignment");
        }

        topicToSubscribe = newTopicsToSubscribe;
        subscriptionChanged.set(true);
    }

    public boolean isSubscribeRequestProcessed() {
        return !subscriptionChanged.get();
    }

    @Override
    public boolean initiateShutdown() {
        boolean res = super.initiateShutdown();
        consumer.wakeup();
        return res;
    }

    @Override
    public void doWork() {
        if (subscriptionChanged.get()) {
            consumer.subscribe(new ArrayList<>(topicToSubscribe), rebalanceListener);
            subscriptionChanged.set(false);
        }
        try {
            receivedMessages.addAndGet(consumer.poll(Duration.ofMillis(50)).count());
        } catch (WakeupException e) {
            // ignore for shutdown
        } catch (Throwable e) {
            thrownException = Optional.of(e);
            throw e;
        }
    }
}
