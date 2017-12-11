package com.lucanet.packratcollector.consumers;

import com.lucanet.packratcollector.config.PackratCollectorConfig;
import com.lucanet.packratcollector.model.HealthCheckHeader;
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
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class FileMessageConsumer implements MessageConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileMessageConsumer.class);

  private final KafkaConsumer<HealthCheckHeader, byte[]> messageConsumer;
  private final List<String> topicsList;
  private final AtomicBoolean isRunning;

  @Autowired
  public FileMessageConsumer(
      PackratCollectorConfig packratCollectorConfig,
      @Value("#{'${packrat.consumers.file.topics}'.split(',')}") List<String> topicsList
  ) {
    Properties props = packratCollectorConfig.generateCommonProperties();
    props.setProperty("value.deserializer", ByteArrayDeserializer.class.getCanonicalName());
    this.messageConsumer = new KafkaConsumer<>(props);
    this.topicsList = topicsList;
    this.isRunning = new AtomicBoolean(false);
  }

  @Override
  public void run() {
    LOGGER.info("Listening to topics {}", topicsList);
    messageConsumer.subscribe(topicsList);
    isRunning.set(true);
    while (isRunning.get()) {
      ConsumerRecords<HealthCheckHeader, byte[]> records = messageConsumer.poll(100L);
      records.forEach(record -> {
        HealthCheckHeader healthCheckHeader = record.key();
        LOGGER.info("Received File message at topic '{}': '{}' | {}", record.topic(), healthCheckHeader, record.value().length);
      });
    }
    messageConsumer.close();
  }

  @Override
  public void stop() {
    isRunning.set(false);
  }

}
