package com.lucanet.packratcollector.observers;

import com.lucanet.packratcollector.model.HealthCheckHeader;
import io.reactivex.Observer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface RecordObserver<T> extends Observer<ConsumerRecord<HealthCheckHeader, T>> {
}
