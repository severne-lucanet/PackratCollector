package com.lucanet.packratcollector.model.deserializers;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

public class JSONDeserializer implements Deserializer<Map<String, Object>> {

  private final Logger logger = LoggerFactory.getLogger(JSONDeserializer.class);
  private final ObjectMapper objectMapper;
  private final TypeReference<HashMap<String, Object>> typeReference;
  private final StringDeserializer stringDeserializer;

  public JSONDeserializer() {
    objectMapper = new ObjectMapper();
    typeReference = new TypeReference<HashMap<String, Object>>(){};
    stringDeserializer = new StringDeserializer();
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    stringDeserializer.configure(configs, isKey);
  }

  @Override
  public Map<String, Object> deserialize(String topic, byte[] data) {
    try {
      return objectMapper.readValue(data, typeReference);
    } catch (Exception e) {
      logger.error("Error parsing value for '{}' message: {}", topic, e.getMessage());
      return null;
    }
  }

  @Override
  public void close() {
    stringDeserializer.close();
  }
}
