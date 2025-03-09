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
package org.apache.kafka.server.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig;
import org.apache.kafka.coordinator.share.ShareCoordinatorConfig;
import org.apache.kafka.coordinator.transaction.AddPartitionsToTxnConfig;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.coordinator.transaction.TransactionStateManagerConfig;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.raft.QuorumConfig;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.server.metrics.MetricConfigs;
import org.apache.kafka.storage.internals.log.CleanerConfig;
import org.apache.kafka.storage.internals.log.LogConfig;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * During moving {@link kafka.server.KafkaConfig} out of core AbstractKafkaConfig will be the future KafkaConfig
 * so any new getters, or updates to `CONFIG_DEF` will be defined here.
 * Any code depends on kafka.server.KafkaConfig will keep for using kafka.server.KafkaConfig for the time being until we move it out of core
 * For more details check KAFKA-15853
 */
public abstract class AbstractKafkaConfig extends AbstractConfig {
    public static final ConfigDef CONFIG_DEF = Utils.mergeConfigs(List.of(
        RemoteLogManagerConfig.configDef(),
        ServerConfigs.CONFIG_DEF,
        KRaftConfigs.CONFIG_DEF,
        SocketServerConfigs.CONFIG_DEF,
        ReplicationConfigs.CONFIG_DEF,
        GroupCoordinatorConfig.CONFIG_DEF,
        CleanerConfig.CONFIG_DEF,
        LogConfig.SERVER_CONFIG_DEF,
        ShareGroupConfig.CONFIG_DEF,
        ShareCoordinatorConfig.CONFIG_DEF,
        TransactionLogConfig.CONFIG_DEF,
        TransactionStateManagerConfig.CONFIG_DEF,
        QuorumConfig.CONFIG_DEF,
        MetricConfigs.CONFIG_DEF,
        QuotaConfig.CONFIG_DEF,
        BrokerSecurityConfigs.CONFIG_DEF,
        DelegationTokenManagerConfigs.CONFIG_DEF,
        AddPartitionsToTxnConfig.CONFIG_DEF
    ));

    private final RemoteLogManagerConfig remoteLogManagerConfig = new RemoteLogManagerConfig(this);

    public AbstractKafkaConfig(ConfigDef definition, Map<?, ?> originals, Map<String, ?> configProviderProps, boolean doLog) {
        super(definition, originals, configProviderProps, doLog);
    }

    public int numIoThreads() {
        return getInt(ServerConfigs.NUM_IO_THREADS_CONFIG);
    }

    public int numReplicaFetchers() {
        return getInt(ReplicationConfigs.NUM_REPLICA_FETCHERS_CONFIG);
    }

    public int numRecoveryThreadsPerDataDir() {
        return getInt(ServerLogConfigs.NUM_RECOVERY_THREADS_PER_DATA_DIR_CONFIG);
    }

    public int backgroundThreads() {
        return getInt(ServerConfigs.BACKGROUND_THREADS_CONFIG);
    }

    public long logRetentionBytes() {
        return getLong(ServerLogConfigs.LOG_RETENTION_BYTES_CONFIG);
    }

    public RemoteLogManagerConfig remoteLogManagerConfig() {
        return remoteLogManagerConfig;
    }

    public int logSegmentBytes() {
        return getInt(ServerLogConfigs.LOG_SEGMENT_BYTES_CONFIG);
    }

    public long logRollTimeMillis() {
        return Optional.ofNullable(getLong(ServerLogConfigs.LOG_ROLL_TIME_MILLIS_CONFIG))
            .orElseGet(() -> 60 * 60 * 1000L * getInt(ServerLogConfigs.LOG_ROLL_TIME_HOURS_CONFIG));
    }

    public long logRollTimeJitterMillis() {
        return Optional.ofNullable(getLong(ServerLogConfigs.LOG_ROLL_TIME_JITTER_MILLIS_CONFIG))
            .orElseGet(() -> 60 * 60 * 1000L * getInt(ServerLogConfigs.LOG_ROLL_TIME_JITTER_HOURS_CONFIG));
    }

    public int logIndexSizeMaxBytes() {
        return getInt(ServerLogConfigs.LOG_INDEX_SIZE_MAX_BYTES_CONFIG);
    }

    public long logFlushIntervalMessages() {
        return getLong(ServerLogConfigs.LOG_FLUSH_INTERVAL_MESSAGES_CONFIG);
    }

    public long logFlushIntervalMs() {
        return Optional.of(getLong(ServerLogConfigs.LOG_FLUSH_INTERVAL_MS_CONFIG))
            .orElseGet(() -> getLong(ServerLogConfigs.LOG_FLUSH_SCHEDULER_INTERVAL_MS_CONFIG));
    }

    public int messageMaxBytes() {
        return getInt(ServerConfigs.MESSAGE_MAX_BYTES_CONFIG);
    }

    public int logIndexIntervalBytes() {
        return getInt(ServerLogConfigs.LOG_INDEX_INTERVAL_BYTES_CONFIG);
    }

    public long logCleanerDeleteRetentionMs() {
        return getLong(CleanerConfig.LOG_CLEANER_DELETE_RETENTION_MS_PROP);
    }

    public long logCleanerMinCompactionLagMs() {
        return getLong(CleanerConfig.LOG_CLEANER_MIN_COMPACTION_LAG_MS_PROP);
    }

    public long logCleanerMaxCompactionLagMs() {
        return getLong(CleanerConfig.LOG_CLEANER_MAX_COMPACTION_LAG_MS_PROP);
    }

    public long logDeleteDelayMs() {
        return getLong(ServerLogConfigs.LOG_DELETE_DELAY_MS_CONFIG);
    }

    public double logCleanerMinCleanRatio() {
        return getDouble(CleanerConfig.LOG_CLEANER_MIN_CLEAN_RATIO_PROP);
    }

    public List<String> logCleanupPolicy() {
        return getList(ServerLogConfigs.LOG_CLEANUP_POLICY_CONFIG);
    }

    public int minInSyncReplicas() {
        return getInt(ServerLogConfigs.MIN_IN_SYNC_REPLICAS_CONFIG);
    }

    public String compressionType() {
        return getString(ServerConfigs.COMPRESSION_TYPE_CONFIG);
    }

    public int gzipCompressionLevel() {
        return getInt(ServerConfigs.COMPRESSION_GZIP_LEVEL_CONFIG);
    }

    public int lz4CompressionLevel() {
        return getInt(ServerConfigs.COMPRESSION_LZ4_LEVEL_CONFIG);
    }

    public int zstdCompressionLevel() {
        return getInt(ServerConfigs.COMPRESSION_ZSTD_LEVEL_CONFIG);
    }

    public boolean uncleanLeaderElectionEnable() {
        return getBoolean(ReplicationConfigs.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG);
    }

    public boolean logPreAllocateEnable() {
        return getBoolean(ServerLogConfigs.LOG_PRE_ALLOCATE_CONFIG);
    }

    public TimestampType logMessageTimestampType() {
        return TimestampType.forName(getString(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_TYPE_CONFIG));
    }

    public long logMessageTimestampBeforeMaxMs() {
        return getLong(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG);
    }

    public long logMessageTimestampAfterMaxMs() {
        return getLong(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG);
    }

    public long logRetentionTimeMillis() {
        long millisInMinute = Duration.ofMinutes(1).toMillis();
        long millisInHour = 60L * millisInMinute;
        long millis = Optional.ofNullable(getLong(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG))
            .orElseGet(() -> Optional.ofNullable(getInt(ServerLogConfigs.LOG_RETENTION_TIME_MINUTES_CONFIG))
                .map(logRetentionMinutes -> logRetentionMinutes * millisInMinute)
                .orElseGet(() -> getInt(ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG) * millisInHour));
        return millis < 0 ? -1 : millis;
    }

    public Map<String, Object> extractLogConfigMap() {
        HashMap<String, Object> logProperties = new HashMap<>();
        logProperties.put(TopicConfig.SEGMENT_BYTES_CONFIG, logSegmentBytes());
        logProperties.put(TopicConfig.SEGMENT_MS_CONFIG, logRollTimeMillis());
        logProperties.put(TopicConfig.SEGMENT_JITTER_MS_CONFIG, logRollTimeJitterMillis());
        logProperties.put(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, logIndexSizeMaxBytes());
        logProperties.put(TopicConfig.FLUSH_MESSAGES_INTERVAL_CONFIG, logFlushIntervalMessages());
        logProperties.put(TopicConfig.FLUSH_MS_CONFIG, logFlushIntervalMs());
        logProperties.put(TopicConfig.RETENTION_BYTES_CONFIG, logRetentionBytes());
        logProperties.put(TopicConfig.RETENTION_MS_CONFIG, logRetentionTimeMillis());
        logProperties.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, messageMaxBytes());
        logProperties.put(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, logIndexIntervalBytes());
        logProperties.put(TopicConfig.DELETE_RETENTION_MS_CONFIG, logCleanerDeleteRetentionMs());
        logProperties.put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, logCleanerMinCompactionLagMs());
        logProperties.put(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, logCleanerMaxCompactionLagMs());
        logProperties.put(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, logDeleteDelayMs());
        logProperties.put(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, logCleanerMinCleanRatio());
        logProperties.put(TopicConfig.CLEANUP_POLICY_CONFIG, logCleanupPolicy());
        logProperties.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minInSyncReplicas());
        logProperties.put(TopicConfig.COMPRESSION_TYPE_CONFIG, compressionType());
        logProperties.put(TopicConfig.COMPRESSION_GZIP_LEVEL_CONFIG, gzipCompressionLevel());
        logProperties.put(TopicConfig.COMPRESSION_LZ4_LEVEL_CONFIG, lz4CompressionLevel());
        logProperties.put(TopicConfig.COMPRESSION_ZSTD_LEVEL_CONFIG, zstdCompressionLevel());
        logProperties.put(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, uncleanLeaderElectionEnable());
        logProperties.put(TopicConfig.PREALLOCATE_CONFIG, logPreAllocateEnable());
        logProperties.put(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, logMessageTimestampType().name);
        logProperties.put(TopicConfig.MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG, logMessageTimestampBeforeMaxMs());
        logProperties.put(TopicConfig.MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG, logMessageTimestampAfterMaxMs());
        logProperties.put(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, remoteLogManagerConfig.logLocalRetentionMs());
        logProperties.put(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, remoteLogManagerConfig.logLocalRetentionBytes());
        return logProperties;
    }
}
