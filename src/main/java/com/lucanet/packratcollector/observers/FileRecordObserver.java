package com.lucanet.packratcollector.observers;

import com.lucanet.packratcollector.model.HealthCheckHeader;
import io.reactivex.Observer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface FileRecordObserver extends Observer<ConsumerRecord<HealthCheckHeader, byte[]>> {
}
