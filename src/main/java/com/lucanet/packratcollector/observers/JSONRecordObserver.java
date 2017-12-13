package com.lucanet.packratcollector.observers;

import com.fasterxml.jackson.databind.JsonNode;
import com.lucanet.packratcollector.model.HealthCheckHeader;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface JSONRecordObserver extends RecordObserver<ConsumerRecord<HealthCheckHeader, JsonNode>> {
}
