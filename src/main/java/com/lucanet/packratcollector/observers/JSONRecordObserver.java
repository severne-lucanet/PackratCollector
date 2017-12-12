package com.lucanet.packratcollector.observers;

import com.lucanet.packratcollector.model.HealthCheckHeader;
import io.reactivex.Observer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.json.JSONObject;

public interface JSONRecordObserver extends Observer<ConsumerRecord<HealthCheckHeader, JSONObject>> {
}
