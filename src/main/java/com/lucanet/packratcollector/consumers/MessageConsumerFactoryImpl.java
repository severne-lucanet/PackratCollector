package com.lucanet.packratcollector.consumers;

import com.lucanet.packratcollector.config.PackratCollectorConfig;
import com.lucanet.packratcollector.db.DatabaseConnection;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Properties;

@Component
public class MessageConsumerFactoryImpl implements MessageConsumerFactory {
  // =========================== Class Variables ===========================79
  // ============================ Class Methods ============================79
  // ============================   Variables    ===========================79
  private final PackratCollectorConfig packratCollectorConfig;
  private final DatabaseConnection databaseConnection;

  // ============================  Constructors  ===========================79
  public MessageConsumerFactoryImpl(PackratCollectorConfig packratCollectorConfig, DatabaseConnection databaseConnection) {
    this.packratCollectorConfig = packratCollectorConfig;
    this.databaseConnection = databaseConnection;
  }

  // ============================ Public Methods ===========================79
  @Override
  public <T> MessageConsumer createMessageConsumer(String consumerName, Class<? extends Deserializer> valueDeserializerClass, List<String> topicsList, int threadpoolSize) {
    Properties messageConsumerProperties = packratCollectorConfig.generateCommonProperties();
    messageConsumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass.getCanonicalName());
    return new MessageConsumerImpl<T>(consumerName, messageConsumerProperties, topicsList, threadpoolSize, databaseConnection);
  }

  // ========================== Protected Methods ==========================79
  // =========================== Private Methods ===========================79
}
