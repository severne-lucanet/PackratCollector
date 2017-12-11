package com.lucanet.packratcollector.model;

public class HealthCheckHeader {

  private String systemUUID;
  private long sessionTimestamp;

  public HealthCheckHeader() {
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

  @Override
  public String toString() {
    return String.format("%s@%d", systemUUID, sessionTimestamp);
  }
}
