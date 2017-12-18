package com.lucanet.packratcollector.db;

import com.lucanet.packratcollector.model.HealthCheckHeader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

public interface DatabaseConnection {
  // =========================== Class Variables ===========================79
  // =============================  Variables  =============================79
  // ============================  Constructors  ===========================79
  // ============================ Public Methods ===========================79
  void persistRecord(ConsumerRecord<HealthCheckHeader, Map<String, Object>> record) throws IllegalArgumentException;
  long getOffset(TopicPartition partition) throws IllegalArgumentException;
  void updateOffset(TopicPartition partition, long newOffset) throws IllegalArgumentException;

  // ========================== Protected Methods ==========================79
  // =========================== Private Methods ===========================79
}
