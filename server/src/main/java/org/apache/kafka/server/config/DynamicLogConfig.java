package org.apache.kafka.server.config;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import kafka.log.LogManager;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DynamicLogConfig implements BrokerReconfigurable {
    private static final Set<String> RECONFIGURABLE_CONFIGS = ServerTopicConfigSynonyms.TOPIC_CONFIG_SYNONYMS.values()
        .stream().collect(Collectors.toUnmodifiableSet());

    private final LogManager logManager;

    public DynamicLogConfig(LogManager logManager) {
        this.logManager = logManager;
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return RECONFIGURABLE_CONFIGS;
    }

    @Override
    public void validateReconfiguration(AbstractKafkaConfig newConfig) {
        // For update of topic config overrides, only config names and types are validated
        // Names and types have already been validated. For consistency with topic config
        // validation, no additional validation is performed.
        validateLogLocalRetentionMs(newConfig);
        validateLogLocalRetentionBytes(newConfig);
    }

    @Override
    public void reconfigure(AbstractKafkaConfig oldConfig, AbstractKafkaConfig newConfig) {
        Map<String, Object> newBrokerDefaults = new HashMap<>(newConfig.extractLogConfigMap());
    }

    private void validateLogLocalRetentionMs(AbstractKafkaConfig newConfig) {
        long logRetentionMs = newConfig.logRetentionTimeMillis();
        long logLocalRetentionMs = newConfig.remoteLogManagerConfig().logLocalRetentionMs();
        if (logRetentionMs != -1L && logLocalRetentionMs != -2L) {
            if (logLocalRetentionMs == -1L) {
                throw new ConfigException(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP, logLocalRetentionMs,
                    "Value must not be -1 as ${ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG} value is set as " + logRetentionMs);
            }
            if (logLocalRetentionMs > logRetentionMs) {
                throw new ConfigException(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP, logLocalRetentionMs,
                    "Value must not be more than ${ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG} property value: " + logRetentionMs);
            }
        }
    }

    private void validateLogLocalRetentionBytes(AbstractKafkaConfig newConfig) {
        long logRetentionBytes = newConfig.logRetentionBytes();
        long logLocalRetentionBytes = newConfig.remoteLogManagerConfig().logLocalRetentionBytes();
        if (logRetentionBytes > -1 && logLocalRetentionBytes != -2) {
            if (logLocalRetentionBytes == -1) {
                throw new ConfigException(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP, logLocalRetentionBytes,
                    "Value must not be -1 as ${ServerLogConfigs.LOG_RETENTION_BYTES_CONFIG} value is set as " + logRetentionBytes);
            }
            if (logLocalRetentionBytes > logRetentionBytes) {
                throw new ConfigException(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP, logLocalRetentionBytes,
                    "Value must not be more than ${ServerLogConfigs.LOG_RETENTION_BYTES_CONFIG} property value: " + logRetentionBytes);
            }
        }
    }
}
