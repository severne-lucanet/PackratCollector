package com.lucanet.packratcollector.model.deserializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lucanet.packratcollector.model.HealthCheckHeader;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class HealthCheckHeaderDeserializer implements Deserializer<HealthCheckHeader> {

  private static final Logger LOGGER = LoggerFactory.getLogger(HealthCheckHeaderDeserializer.class);

  private final ObjectMapper objectMapper;

  public HealthCheckHeaderDeserializer() {
    objectMapper = new ObjectMapper();
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    //No-Op
  }

  @Override
  public HealthCheckHeader deserialize(String topic, byte[] data) {
    try {
      return objectMapper.readValue(data, HealthCheckHeader.class);
    } catch (IOException ioe) {
      LOGGER.error("Error deserializing HealthCheckHeader: {}", ioe.getMessage());
      return null;
    }
  }

  @Override
  public void close() {
    //No-Op
  }
}
