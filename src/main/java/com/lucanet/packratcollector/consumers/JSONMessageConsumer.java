package com.lucanet.packratcollector.consumers;

import com.lucanet.packratcollector.config.PackratCollectorConfig;
import com.lucanet.packratcollector.model.HealthCheckHeader;
import com.lucanet.packratcollector.model.deserializers.JSONDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class JSONMessageConsumer implements MessageConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(JSONMessageConsumer.class);

  private final KafkaConsumer<HealthCheckHeader, JSONObject> messageConsumer;
  private final List<String> topicsList;
  private final AtomicBoolean isRunning;

  public JSONMessageConsumer(
      PackratCollectorConfig packratCollectorConfig,
      @Value("#{'${packrat.consumers.json.topics}'.split(',')}") List<String> topicsList
  ) {
    Properties props = packratCollectorConfig.generateCommonProperties();
    props.setProperty("value.deserializer", JSONDeserializer.class.getCanonicalName());
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
      ConsumerRecords<HealthCheckHeader, JSONObject> records = messageConsumer.poll(100L);
      records.forEach(record -> {
        HealthCheckHeader healthCheckHeader = record.key();
        LOGGER.info("Received JSON message at topic '{}': '{}' | {}", record.topic(), healthCheckHeader, record.value());
      });
    }
    messageConsumer.close();
  }

  @Override
  public void stop() {
    isRunning.set(false);
  }

}
