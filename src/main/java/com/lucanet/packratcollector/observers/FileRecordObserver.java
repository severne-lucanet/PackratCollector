package com.lucanet.packratcollector.observers;

import com.lucanet.packratcollector.model.HealthCheckHeader;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface FileRecordObserver extends RecordObserver<ConsumerRecord<HealthCheckHeader, byte[]>> {
}
