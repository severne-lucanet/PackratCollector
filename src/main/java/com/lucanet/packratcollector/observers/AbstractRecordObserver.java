package com.lucanet.packratcollector.observers;

import com.lucanet.packratcollector.db.DatabaseConnection;
import com.lucanet.packratcollector.model.HealthCheckHeader;
import io.reactivex.disposables.Disposable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractRecordObserver<T> implements RecordObserver<T> {

  private final Logger             logger;
  private final DatabaseConnection databaseConnection;

  AbstractRecordObserver(DatabaseConnection databaseConnection) {
    this.logger = LoggerFactory.getLogger(getClass());
    this.databaseConnection = databaseConnection;
  }

  @Override
  public void onSubscribe(Disposable d) {
    //No-Op
  }

  @Override
  public void onNext(ConsumerRecord<HealthCheckHeader, T> consumerRecord) {
    try {
      databaseConnection.updateOffset(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()), (consumerRecord.offset() + 1));
    } catch (IllegalArgumentException iae) {
      logger.error("Unable to persist offset for topic '{}' partition {}: {}", consumerRecord.topic(), consumerRecord.partition(), iae.getMessage());
    }
    if ((consumerRecord.key() != null) && (consumerRecord.value() != null)) {
      logger.debug("Record received for '{}': {}", consumerRecord.topic(), consumerRecord.value());
      try {
        databaseConnection.persistRecord(consumerRecord);
      } catch (IllegalArgumentException iae) {
        logger.error("Unable to write '{}' record {}@{}: topic does not exist in database", consumerRecord.topic(), consumerRecord.offset(), consumerRecord.timestamp());
      }
    } else {
      logger.warn("Unable to process '{}' record {}@{}: either key or value were null", consumerRecord.topic(), consumerRecord.offset(), consumerRecord.timestamp());
    }
  }

  @Override
  public void onError(Throwable e) {
    logger.error("Error in processing JSON Record: {}", e.getMessage());
  }

  @Override
  public void onComplete() {
    //No-Op
  }

}
