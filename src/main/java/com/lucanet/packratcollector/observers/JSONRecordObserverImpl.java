package com.lucanet.packratcollector.observers;

import com.lucanet.packratcollector.model.HealthCheckHeader;
import com.lucanet.packratcollector.persister.RecordPersister;
import io.reactivex.disposables.Disposable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class JSONRecordObserverImpl implements JSONRecordObserver {

  private static final Logger LOGGER = LoggerFactory.getLogger(JSONRecordObserverImpl.class);

  private final RecordPersister recordPersister;

  @Autowired
  public JSONRecordObserverImpl(RecordPersister recordPersister) {
    this.recordPersister = recordPersister;
  }

  @Override
  public void onSubscribe(Disposable d) {
    //No-Op
  }

  @Override
  public void onNext(ConsumerRecord<HealthCheckHeader, JSONObject> consumerRecord) {
    if ((consumerRecord.key() != null) && (consumerRecord.value() != null)) {
      recordPersister.persistJSONRecord(consumerRecord);
    } else {
      LOGGER.warn("Unable to process '{}' record {}@{}: either key or value were null", consumerRecord.topic(), consumerRecord.offset(), consumerRecord.timestamp());
    }
  }

  @Override
  public void onError(Throwable e) {
    LOGGER.error("Error in processing JSON Record: {}", e.getMessage());
  }

  @Override
  public void onComplete() {
    //No-Op
  }
}
