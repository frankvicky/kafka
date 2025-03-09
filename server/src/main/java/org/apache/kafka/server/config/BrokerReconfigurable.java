package org.apache.kafka.server.config;

import java.util.Set;

public interface BrokerReconfigurable {
    Set<String> reconfigurableConfigs();
    void validateReconfiguration(AbstractKafkaConfig newConfig);
    void reconfigure(AbstractKafkaConfig oldConfig, AbstractKafkaConfig kafkaConfig);
}
