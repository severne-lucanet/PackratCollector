package com.lucanet.packratcollector.consumers;

import com.lucanet.packratcollector.db.DatabaseConnection;
import com.lucanet.packratcollector.model.HealthCheckHeader;
import com.lucanet.packratcollector.observers.RecordObserver;
import io.reactivex.Observable;
import io.reactivex.schedulers.Schedulers;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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

abstract class AbstractMessageConsumer<T> implements MessageConsumer {
  // =========================== Class Variables ===========================79
  // ============================ Class Methods ============================79
  // ============================   Variables    ===========================79
  private final Logger                              logger;
  private final KafkaConsumer<HealthCheckHeader, T> messageConsumer;
  private final AtomicBoolean                       isRunning;
  private final List<String>                        topicsList;
  private final ExecutorService                     threadPoolExecutor;
  private final RecordObserver<T>                   recordObserver;
  private final DatabaseConnection                  databaseConnection;
  private final Thread                              runnerThread;

  // ============================  Constructors  ===========================79
  AbstractMessageConsumer(
      Properties commonProperties,
      Class<?> valueDeserializerClass,
      List<String> topicsList,
      int threadPoolSize,
      RecordObserver<T> recordObserver,
      DatabaseConnection databaseConnection
  ) {
    this.logger = LoggerFactory.getLogger(getClass());
    commonProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass.getCanonicalName());
    this.messageConsumer = new KafkaConsumer<>(commonProperties);
    this.isRunning = new AtomicBoolean(false);
    this.topicsList = topicsList;
    this.threadPoolExecutor = Executors.newFixedThreadPool(threadPoolSize);
    this.recordObserver = recordObserver;
    this.databaseConnection = databaseConnection;
    this.runnerThread = new Thread(this::runConsumer);
  }

  // ============================ Public Methods ===========================79
  @Override
  public void run() {
    runnerThread.run();
  }

  @Override
  public void stop() {
    logger.info("Shutting down consumer...");
    this.isRunning.set(false);
    try {
      runnerThread.join();
    } catch (InterruptedException ie) {
      //No-Op
    }
    logger.info("Consumer shut down");
  }

  // ========================== Protected Methods ==========================79
  // =========================== Private Methods ===========================79
  private void runConsumer() {
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
}
