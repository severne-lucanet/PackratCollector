package com.lucanet.packratcollector.services;

import org.apache.kafka.common.TopicPartition;

public interface OffsetLookupService {
  long getOffset(TopicPartition topicPartition);
  void saveOffset(TopicPartition topicPartition, long offset);
}
