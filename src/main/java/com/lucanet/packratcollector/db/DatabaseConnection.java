package com.lucanet.packratcollector.db;

import com.lucanet.packratcollector.model.HealthCheckHeader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;

public interface DatabaseConnection {
  // ========================= Interface Variables =========================79
  // ============================ Public Methods ===========================79
  <T> void persistRecord(ConsumerRecord<HealthCheckHeader, T> record) throws IllegalArgumentException;
  long getOffset(TopicPartition partition) throws IllegalArgumentException;
  void updateOffset(TopicPartition partition, long newOffset) throws IllegalArgumentException;
  List<String> getTopics();
  List<String> getSystemsInTopic(String topicName) throws IllegalArgumentException;
  List<Long> getSessionTimestamps(String topicName, String systemUUID) throws IllegalArgumentException;
  List<Map<String, Object>> getSessionHealthChecks(String topicName, String systemUUID, Long sessionTimestamp) throws IllegalArgumentException;
  Map<String, List<String>> getSerialIDS();
  Map<String, List<String>> getSystemsForSerialID(String serialID);

  // =========================== Default Methods ===========================79
}
