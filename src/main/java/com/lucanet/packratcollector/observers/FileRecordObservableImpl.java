package com.lucanet.packratcollector.observers;

import com.lucanet.packratcollector.model.HealthCheckHeader;
import io.reactivex.disposables.Disposable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class FileRecordObservableImpl implements FileRecordObserver {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileRecordObservableImpl.class);

  @Autowired
  public FileRecordObservableImpl() {
  }

  @Override
  public void onSubscribe(Disposable d) {
    //No-Op
  }

  @Override
  public void onNext(ConsumerRecord<HealthCheckHeader, byte[]> consumerRecord) {
    if ((consumerRecord.key() != null) && ((consumerRecord.value() != null) && (consumerRecord.value().length > 0))) {
      //TODO: Upload to Elastic Stack
    } else {
      LOGGER.warn("Unable to process '{}' record {}@{}: either key or value were null", consumerRecord.topic(), consumerRecord.offset(), consumerRecord.timestamp());
    }
  }

  @Override
  public void onError(Throwable e) {
    LOGGER.error("Error in processing File Record: {}", e.getMessage());
  }

  @Override
  public void onComplete() {
    //No-Op
  }
}
