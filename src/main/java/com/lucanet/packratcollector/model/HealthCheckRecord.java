package com.lucanet.packratcollector.model;

import com.fasterxml.jackson.databind.JsonNode;
import org.bson.Document;

import java.util.HashMap;
import java.util.Map;

/**
 * Wrapper class for Document to represent a HealthCheck entity for usage in the Packrat Collector
 */
public class HealthCheckRecord extends Document {
  // =========================== Class Variables ===========================79
  private static final String SYSTEM_UUID           = "systemUUID";
  private static final String SESSION_TIMESTAMP     = "sessionTimestamp";
  private static final String HEALTHCHECK_TIMESTAMP = "healthCheckTimestamp";
  private static final String DATA                  = "data";

  // ============================ Class Methods ============================79
  // ============================   Variables    ===========================79
  // ============================  Constructors  ===========================79
  public HealthCheckRecord(HealthCheckHeader healthCheckHeader, Map<String, Object> data) {
    put("_id", healthCheckHeader.toString());
    put(SYSTEM_UUID, healthCheckHeader.getSystemUUID());
    put(SESSION_TIMESTAMP, healthCheckHeader.getSessionTimestamp());
    put(HEALTHCHECK_TIMESTAMP, healthCheckHeader.getHealthCheckTimestamp());
    put(DATA, data);
  }

  // ============================ Public Methods ===========================79
  public String getSystemUUID() {
    return getString(SYSTEM_UUID);
  }

  public void setSystemUUID(String systemUUID) {
    put(SYSTEM_UUID, systemUUID);
  }

  public long getSessionTimestamp() {
    return getLong(SESSION_TIMESTAMP);
  }

  public void setSessionTimestamp(long sessionTimestamp) {
    put(SESSION_TIMESTAMP, sessionTimestamp);
  }

  public long getHealthCheckTimestamp() {
    return getLong(HEALTHCHECK_TIMESTAMP);
  }

  public void setHealthCheckTimestamp(long healthCheckTimestamp) {
    put(HEALTHCHECK_TIMESTAMP, healthCheckTimestamp);
  }

  @SuppressWarnings("unchecked")
  public Map<String, Object> getData() {
    return (HashMap<String, Object>) get(DATA, HashMap.class);
  }

  public void setData(JsonNode data) {
    put(DATA, data);
  }

  // ========================== Protected Methods ==========================79
  // =========================== Private Methods ===========================79
}
