package com.lucanet.packratcollector.persister;

import com.fasterxml.jackson.databind.JsonNode;
import com.lucanet.packratcollector.model.HealthCheckHeader;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface RecordPersister {
  void persistJSONRecord(ConsumerRecord<HealthCheckHeader, JsonNode> record);
  void persistFileRecord(ConsumerRecord<HealthCheckHeader, byte[]> record);
}
