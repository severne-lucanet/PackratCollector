package com.lucanet.packratcollector.model.deserializers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;

public class JSONDeserializer implements Deserializer<JsonNode> {

  private final ObjectMapper objectMapper;
  private final StringDeserializer stringDeserializer;

  public JSONDeserializer() {
    objectMapper = new ObjectMapper();
    stringDeserializer = new StringDeserializer();
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    stringDeserializer.configure(configs, isKey);
  }

  @Override
  public JsonNode deserialize(String topic, byte[] data) {
    return objectMapper.convertValue(stringDeserializer.deserialize(topic, data), JsonNode.class);
  }

  @Override
  public void close() {
    stringDeserializer.close();
  }
}
