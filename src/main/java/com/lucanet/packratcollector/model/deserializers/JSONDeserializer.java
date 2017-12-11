package com.lucanet.packratcollector.model.deserializers;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

public class JSONDeserializer implements Deserializer<JSONObject> {

  private static final Logger LOGGER = LoggerFactory.getLogger(JSONDeserializer.class);

  private final StringDeserializer stringDeserializer;

  public JSONDeserializer() {
    stringDeserializer = new StringDeserializer();
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    stringDeserializer.configure(configs, isKey);
  }

  @Override
  public JSONObject deserialize(String topic, byte[] data) {
    try {
      return new JSONObject(stringDeserializer.deserialize(topic, data));
    } catch (JSONException jsone) {
      LOGGER.error("Error parsing JSON data for topic '{}': {}", topic, jsone.getMessage());
      return null;
    }
  }

  @Override
  public void close() {
    stringDeserializer.close();
  }
}
