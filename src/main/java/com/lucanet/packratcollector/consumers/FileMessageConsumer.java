package com.lucanet.packratcollector.consumers;

import com.lucanet.packratcollector.config.PackratCollectorConfig;
import com.lucanet.packratcollector.model.HealthCheckHeader;
import com.lucanet.packratcollector.observers.FileRecordObserver;
import io.reactivex.Observable;
import io.reactivex.schedulers.Schedulers;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class FileMessageConsumer implements MessageConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileMessageConsumer.class);

  private final KafkaConsumer<HealthCheckHeader, byte[]> messageConsumer;
  private final FileRecordObserver fileRecordObserver;
  private final List<String> topicsList;
  private final ExecutorService threadPoolExecutor;
  private final AtomicBoolean isRunning;

  @Autowired
  public FileMessageConsumer(
      PackratCollectorConfig packratCollectorConfig,
      @Value("#{'${packrat.consumers.file.topics}'.split(',')}") List<String> topicsList,
      @Value("${packrat.consumers.file.threadpoolsize}") int threadPoolSize,
      FileRecordObserver fileRecordObserver
  ) {
    Properties props = packratCollectorConfig.generateCommonProperties();
    props.setProperty("value.deserializer", ByteArrayDeserializer.class.getCanonicalName());
    this.messageConsumer = new KafkaConsumer<>(props);
    this.topicsList = topicsList;
    this.threadPoolExecutor = Executors.newFixedThreadPool(threadPoolSize);
    this.fileRecordObserver = fileRecordObserver;
    this.isRunning = new AtomicBoolean(false);
  }

  @Override
  public void run() {
    LOGGER.info("FileMessageConsumer listening to topics {}", topicsList);
    messageConsumer.subscribe(topicsList);
    isRunning.set(true);
    while (isRunning.get()) {
      ConsumerRecords<HealthCheckHeader, byte[]> records = messageConsumer.poll(100L);
      Observable.fromIterable(records)
          .subscribeOn(Schedulers.from(threadPoolExecutor))
          .subscribe(fileRecordObserver);
    }
    messageConsumer.close();
  }

  @Override
  public void stop() {
    isRunning.set(false);
  }

}
