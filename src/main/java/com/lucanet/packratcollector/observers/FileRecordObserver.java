package com.lucanet.packratcollector.observers;

import com.lucanet.packratcollector.model.HealthCheckHeader;
import io.reactivex.disposables.Disposable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class FileRecordObserver implements RecordObserver<byte[]> {
  // =========================== Class Variables ===========================79
  // ============================ Class Methods ============================79
  // ============================   Variables    ===========================79
  private final Logger logger;

  // ============================  Constructors  ===========================79
  @Autowired
  public FileRecordObserver() {
    logger = LoggerFactory.getLogger(FileRecordObserver.class);
  }

  // ============================ Public Methods ===========================79
  @Override
  public void onSubscribe(Disposable d) {
    //No-Op
  }

  @Override
  public void onNext(ConsumerRecord<HealthCheckHeader, byte[]> consumerRecord) {
    if ((consumerRecord.key() != null) && ((consumerRecord.value() != null) && (consumerRecord.value().length > 0))) {
      //TODO: Upload to Elastic Stack
    } else {
      logger.warn("Unable to process '{}' record {}@{}: either key or value were null", consumerRecord.topic(), consumerRecord.offset(), consumerRecord.timestamp());
    }
  }

  @Override
  public void onError(Throwable e) {
    logger.error("Error in processing File Record: {}", e.getMessage());
  }

  @Override
  public void onComplete() {
    //No-Op
  }

  // ========================== Protected Methods ==========================79
  // =========================== Private Methods ===========================79
}
