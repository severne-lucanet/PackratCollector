package com.lucanet.packratcollector.model;

import com.fasterxml.jackson.databind.JsonNode;
import org.ektorp.support.CouchDbDocument;

public class HealthCheckRecord extends CouchDbDocument {

  private String systemUUID;
  private long sessionTimestamp;
  private long healthCheckTimestamp;
  private JsonNode data;

  public HealthCheckRecord(HealthCheckHeader healthCheckHeader, JsonNode data) {
    this.systemUUID = healthCheckHeader.getSystemUUID();
    this.sessionTimestamp = healthCheckHeader.getSessionTimestamp();
    this.healthCheckTimestamp = healthCheckHeader.getHealthCheckTimestamp();
    setId(healthCheckHeader.toString());
    this.data = data;
  }

  public String getSystemUUID() {
    return systemUUID;
  }

  public void setSystemUUID(String systemUUID) {
    this.systemUUID = systemUUID;
  }

  public long getSessionTimestamp() {
    return sessionTimestamp;
  }

  public void setSessionTimestamp(long sessionTimestamp) {
    this.sessionTimestamp = sessionTimestamp;
  }

  public long getHealthCheckTimestamp() {
    return healthCheckTimestamp;
  }

  public void setHealthCheckTimestamp(long healthCheckTimestamp) {
    this.healthCheckTimestamp = healthCheckTimestamp;
  }

  public JsonNode getData() {
    return data;
  }

  public void setData(JsonNode data) {
    this.data = data;
  }
}
