package com.lucanet.packratcollector.db;

import com.lucanet.packratcollector.model.HealthCheckHeader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

public interface DatabaseConnection {
  void persistRecord(ConsumerRecord<HealthCheckHeader, Map<String, Object>> record) throws IllegalArgumentException;
  long getOffset(TopicPartition partition) throws IllegalArgumentException;
  void updateOffset(TopicPartition partition, long newOffset) throws IllegalArgumentException;
}
