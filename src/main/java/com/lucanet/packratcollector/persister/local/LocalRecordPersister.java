package com.lucanet.packratcollector.persister.local;

import com.lucanet.packratcollector.model.HealthCheckHeader;
import com.lucanet.packratcollector.model.HealthCheckRecord;
import com.lucanet.packratcollector.persister.RecordPersister;
import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.bson.Document;
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

  private final Map<String, MongoCollection<Document>> repoMap;

  @Autowired
  public LocalRecordPersister(
    @Value("${packrat.persister.local.url}") String dbURL,
    @Value("${packrat.persister.local.port}") int dbPort,
    @Value("${packrat.persister.local.username}") String username,
    @Value("${packrat.persister.local.password}") String password,
    @Value("#{'${packrat.consumers.json.topics}'.split(',')}") List<String> topicsList
  ) throws Exception {
    LOGGER.info("Building CouchDB connection to {}:{}@{}:{}", username, password, dbURL, dbPort);
    MongoClientOptions.Builder clientOptionsBuilder = new MongoClientOptions.Builder();
    MongoClient mongoClient = new MongoClient(
        new ServerAddress(dbURL, dbPort),
        MongoCredential.createCredential(username, "packrat_healthcheck", password.toCharArray()),
        clientOptionsBuilder.build()
    );
    MongoDatabase healthCheckDB = mongoClient.getDatabase("packrat_healthcheck");
    LOGGER.info("Creating connections to databases for topics {}", topicsList);
    repoMap = topicsList.stream()
        .map(String::toLowerCase)
        .collect(Collectors.toMap(
            topicName -> topicName,
            healthCheckDB::getCollection)
        );
  }

  @Override
  public void persistJSONRecord(ConsumerRecord<HealthCheckHeader, Map<String, Object>> record) {
    String dbName = record.topic().toLowerCase();
    if (repoMap.containsKey(dbName)) {
      HealthCheckHeader header = record.key();
      HealthCheckRecord healthCheckRecord = new HealthCheckRecord(header, record.value());
      try {
        repoMap.get(dbName).insertOne(healthCheckRecord);
      } catch (MongoWriteException mwe) {
        if (mwe.getError().getCategory() == ErrorCategory.DUPLICATE_KEY) {
          LOGGER.warn("Cannot write message {} - entry already exists with this key for topic '{}'", header, record.topic());
        } else {
          LOGGER.error("Error writing message {} to topic '{}': {}", header, record.topic(), mwe.getMessage());
        }
      } catch (Exception e) {
        LOGGER.error("Error writing message {} to topic '{}': {}", header, record.topic(), e.getMessage());
      }
    } else {
      LOGGER.warn("Database '{}' does not exist - skipping insertion", dbName);
    }
  }

  @Override
  public void persistFileRecord(ConsumerRecord<HealthCheckHeader, byte[]> record) {
    //TODO
  }
}
