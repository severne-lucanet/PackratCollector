package com.lucanet.packratcollector.consumers;

import com.lucanet.packratcollector.db.DatabaseConnection;
import com.lucanet.packratcollector.model.HealthCheckHeader;
import com.lucanet.packratcollector.observers.RecordObserver;
import io.reactivex.Observable;
import io.reactivex.schedulers.Schedulers;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractMessageConsumer<T> implements MessageConsumer {

  private final Logger logger;
  private final KafkaConsumer<HealthCheckHeader, T> messageConsumer;
  private final AtomicBoolean isRunning;
  private final List<String> topicsList;
  private final ExecutorService threadPoolExecutor;
  private final RecordObserver<ConsumerRecord<HealthCheckHeader, T>> recordObserver;
  private final DatabaseConnection databaseConnection;

  AbstractMessageConsumer(
      Properties commonProperties,
      Class<?> valueDeserializerClass,
      List<String> topicsList,
      int threadPoolSize,
      RecordObserver<ConsumerRecord<HealthCheckHeader, T>> recordObserver,
      DatabaseConnection databaseConnection
  ) {
    logger = LoggerFactory.getLogger(getClass());
    commonProperties.setProperty("value.deserializer", valueDeserializerClass.getCanonicalName());
    this.messageConsumer = new KafkaConsumer<>(commonProperties);
    this.isRunning = new AtomicBoolean(false);
    this.topicsList = topicsList;
    this.threadPoolExecutor = Executors.newFixedThreadPool(threadPoolSize);
    this.recordObserver = recordObserver;
    this.databaseConnection = databaseConnection;
  }

  @Override
  public void run() {
    messageConsumer.subscribe(topicsList);
    messageConsumer.poll(0L);
    topicsList.forEach(topic ->
      messageConsumer.partitionsFor(topic).forEach(partitionInfo -> {
        TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
        long topicPartitionOffset = databaseConnection.getOffset(topicPartition);
        logger.info("Setting offset to {} for topic '{}' partition {}", topicPartitionOffset, partitionInfo.topic(), partitionInfo.partition());
        messageConsumer.seek(topicPartition, topicPartitionOffset);
      })
    );
    isRunning.set(true);
    while (isRunning.get()) {
      try {
        logger.debug("Polling Kafka Server...");
        ConsumerRecords<HealthCheckHeader, T> records = messageConsumer.poll(1000L);
        logger.debug("Records Polled: {}", records.count());
        Observable.fromIterable(records)
            .subscribeOn(Schedulers.from(threadPoolExecutor))
            .subscribe(recordObserver);
      } catch (Exception e) {
        logger.error("Error polling messages: {}", e.getMessage());
      }
    }
    messageConsumer.close();
  }

  @Override
  public void stop() {
    this.isRunning.set(false);
  }
}
