package com.lucanet.packratcollector.persister;

import com.lucanet.packratcollector.model.HealthCheckHeader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.json.JSONObject;

public interface RecordPersister {
  void persistJSONRecord(ConsumerRecord<HealthCheckHeader, JSONObject> record);
  void persistFileRecord(ConsumerRecord<HealthCheckHeader, byte[]> record);
}
