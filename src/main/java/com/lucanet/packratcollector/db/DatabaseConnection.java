package com.lucanet.packratcollector.db;

import com.lucanet.packratcollector.model.HealthCheckHeader;
import com.lucanet.packratcollector.model.HealthCheckRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;

public interface DatabaseConnection {
  // =========================== Class Variables ===========================79
  // =============================  Variables  =============================79
  // ============================  Constructors  ===========================79
  // ============================ Public Methods ===========================79
  void persistRecord(ConsumerRecord<HealthCheckHeader, Map<String, Object>> record) throws IllegalArgumentException;
  long getOffset(TopicPartition partition) throws IllegalArgumentException;
  void updateOffset(TopicPartition partition, long newOffset) throws IllegalArgumentException;
  List<String> getTopics();
  List<String> getSystemsInTopic(String topicName);
  List<Long> getSessionTimestamps(String topicName, String systemUUID);
  List<Map<String, Object>> getSessionHealthChecks(String topicName, String systemUUID, Long sessionTimestamp);

  // ========================== Protected Methods ==========================79
  // =========================== Private Methods ===========================79
}
