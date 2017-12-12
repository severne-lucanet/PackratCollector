package com.lucanet.packratcollector.persister.local;

import com.lucanet.packratcollector.model.HealthCheckHeader;
import com.lucanet.packratcollector.model.HealthCheckRecord;
import com.lucanet.packratcollector.persister.RecordPersister;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbInstance;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class LocalRecordPersister implements RecordPersister {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalRecordPersister.class);

  private final StdCouchDbInstance dbInstance;
  private final Map<String, HealthCheckRecordRepo> repoMap;

  @Autowired
  public LocalRecordPersister(
    @Value("${packrat.persister.local.url}") String dbURL,
    @Value("${packrat.persister.local.username}") String username,
    @Value("${packrat.persister.local.password}") String password,
    @Value("#{'${packrat.consumers.json.topics}'.split(',')}") List<String> topicsList
  ) throws Exception {
    LOGGER.info("Building CouchDB connection to {}:{}@{}", username, password, dbURL);
    HttpClient authenticatedHttpClient = new StdHttpClient.Builder()
        .url(dbURL)
        .username(username)
        .password(password)
        .build();
    dbInstance = new StdCouchDbInstance(authenticatedHttpClient);
    LOGGER.info("Creating connections to databases for topics {}", topicsList);
    repoMap = topicsList.stream()
        .map(String::toLowerCase)
        .collect(Collectors.toMap(
            topicName -> topicName,
            topicName -> new HealthCheckRecordRepo(dbInstance.createConnector(topicName, true))
        ));
  }

  @Override
  public void persistJSONRecord(ConsumerRecord<HealthCheckHeader, JSONObject> record) {
    String dbName = record.topic().toLowerCase();
    if (repoMap.containsKey(dbName)) {
      HealthCheckRecord healthCheckRecord = new HealthCheckRecord(record.key().getSystemUUID(), record.key().getSessionTimestamp(), record.value());
      repoMap.get(dbName).add(healthCheckRecord);
    } else {
      LOGGER.warn("Database '{}' does not exist - skipping insertion", dbName);
    }
  }

  @Override
  public void persistFileRecord(ConsumerRecord<HealthCheckHeader, byte[]> record) {
    //TODO
  }
}
