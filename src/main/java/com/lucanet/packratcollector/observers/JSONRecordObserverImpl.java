package com.lucanet.packratcollector.observers;

import com.lucanet.packratcollector.model.HealthCheckHeader;
import com.lucanet.packratcollector.persister.RecordPersister;
import com.lucanet.packratcollector.services.OffsetLookupService;
import io.reactivex.disposables.Disposable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class JSONRecordObserverImpl implements JSONRecordObserver {

  private static final Logger LOGGER = LoggerFactory.getLogger(JSONRecordObserverImpl.class);

  private final RecordPersister recordPersister;
  private final OffsetLookupService offsetLookupService;

  @Autowired
  public JSONRecordObserverImpl(RecordPersister recordPersister, OffsetLookupService offsetLookupService) {
    this.recordPersister = recordPersister;
    this.offsetLookupService = offsetLookupService;
  }

  @Override
  public void onSubscribe(Disposable d) {
    //No-Op
  }

  @Override
  public void onNext(ConsumerRecord<HealthCheckHeader, Map<String, Object>> consumerRecord) {
    offsetLookupService.saveOffset(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()), (consumerRecord.offset() + 1));
    if ((consumerRecord.key() != null) && (consumerRecord.value() != null)) {
      LOGGER.debug("Record received for '{}': {}", consumerRecord.topic(), consumerRecord.value());
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
