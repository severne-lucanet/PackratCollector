package com.lucanet.packratcollector.model;

import org.ektorp.support.CouchDbDocument;
import org.json.JSONObject;

public class HealthCheckRecord extends CouchDbDocument {

  private String systemUUID;
  private long sessionTimestamp;
  private JSONObject data;

  public HealthCheckRecord(String systemUUID, long sessionTimestamp, JSONObject data) {
    this.systemUUID = systemUUID;
    this.sessionTimestamp = sessionTimestamp;
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

  public JSONObject getData() {
    return data;
  }

  public void setData(JSONObject data) {
    this.data = data;
  }
}
