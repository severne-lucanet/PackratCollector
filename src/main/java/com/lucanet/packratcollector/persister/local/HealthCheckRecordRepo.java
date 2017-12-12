package com.lucanet.packratcollector.persister.local;

import com.lucanet.packratcollector.model.HealthCheckRecord;
import org.ektorp.CouchDbConnector;
import org.ektorp.support.CouchDbRepositorySupport;

public class HealthCheckRecordRepo extends CouchDbRepositorySupport<HealthCheckRecord> {

  public HealthCheckRecordRepo(CouchDbConnector dbConnector) {
    super(HealthCheckRecord.class, dbConnector, true);
  }

}
