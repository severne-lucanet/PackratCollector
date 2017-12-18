package com.lucanet.packratcollector.config;

import com.lucanet.packratcollector.model.deserializers.HealthCheckHeaderDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class PackratCollectorConfig {
  // =========================== Class Variables ===========================79
  // ============================ Class Methods ============================79
  // ============================   Variables    ===========================79
  private final String  bootstrapServers;
  private final String  groupId;
  private final Boolean enableAutoCommit;
  private final Long    autoCommitInterval;
  private final Long    sessionTimeout;

  // ============================  Constructors  ===========================79
  public PackratCollectorConfig(
      @Value("${packrat.bootstrapServers}") String bootstrapServers,
      @Value("${packrat.groupId}") String groupId,
      @Value("${packrat.enableAutoCommit}") Boolean enableAutoCommit,
      @Value("${packrat.autoCommitInterval}") Long autoCommitInterval,
      @Value("${packrat.sessionTimeout}") Long sessionTimeout
  ) {
    this.bootstrapServers = bootstrapServers;
    this.groupId = groupId;
    this.enableAutoCommit = enableAutoCommit;
    this.autoCommitInterval = autoCommitInterval;
    this.sessionTimeout = sessionTimeout;
  }

  // ============================ Public Methods ===========================79
  public Properties generateCommonProperties() {
    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrapServers);
    props.put("group.id", groupId);
    props.put("enable.auto.commit", enableAutoCommit.toString());
    props.put("auto.commit.interval.ms", autoCommitInterval.toString());
    props.put("session.timeout.ms", sessionTimeout.toString());
    props.put("key.deserializer", HealthCheckHeaderDeserializer.class.getCanonicalName());
    return props;
  }

  // ========================== Protected Methods ==========================79
  // =========================== Private Methods ===========================79
}
