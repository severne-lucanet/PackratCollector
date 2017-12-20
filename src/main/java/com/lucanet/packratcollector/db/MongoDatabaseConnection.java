package com.lucanet.packratcollector.db;

import com.lucanet.packratcollector.model.HealthCheckHeader;
import com.lucanet.packratcollector.model.HealthCheckRecord;
import com.mongodb.*;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@Service
public class MongoDatabaseConnection implements DatabaseConnection {
  // =========================== Class Variables ===========================79
  private static final String OFFSETS_COLLECTION_NAME = "_offsets";
  private static final String OFFSETS_TOPIC_KEY       = "topic";
  private static final String OFFSETS_PARTITION_KEY   = "partition";
  private static final String OFFSETS_OFFSET_KEY      = "offset";

  // ============================ Class Methods ============================79
  // ============================   Variables    ===========================79
  private final Logger        logger;
  private final MongoDatabase healthCheckDB;

  // ============================  Constructors  ===========================79
  @Autowired
  public MongoDatabaseConnection(
    @Value("${packrat.persister.local.url}") String dbURL,
    @Value("${packrat.persister.local.port}") int dbPort,
    @Value("${packrat.persister.local.username}") String username,
    @Value("${packrat.persister.local.password}") String password
  ) {
    logger = LoggerFactory.getLogger(MongoDatabaseConnection.class);
    logger.info("Building CouchDB connection to {}:{}@{}:{}", username, password, dbURL, dbPort);
    MongoClientOptions.Builder clientOptionsBuilder = new MongoClientOptions.Builder();
    MongoClient mongoClient = new MongoClient(
        new ServerAddress(dbURL, dbPort),
        MongoCredential.createCredential(username, "packrat_healthcheck", password.toCharArray()),
        clientOptionsBuilder.build()
    );
    this.healthCheckDB = mongoClient.getDatabase("packrat_healthcheck");
  }

  // ============================ Public Methods ===========================79
  @Override
  public void persistRecord(ConsumerRecord<HealthCheckHeader, Map<String, Object>> record) throws IllegalArgumentException {
    String collectionName = record.topic().toLowerCase();
    HealthCheckHeader header = record.key();
    HealthCheckRecord healthCheckRecord = new HealthCheckRecord(header, record.value());
    try {
      healthCheckDB.getCollection(collectionName, HealthCheckRecord.class).insertOne(healthCheckRecord);
    } catch (MongoWriteException mwe) {
      if (mwe.getError().getCategory() == ErrorCategory.DUPLICATE_KEY) {
        logger.warn("Cannot write message {} - entry already exists with this key for topic '{}'", header, record.topic());
      } else {
        logger.error("Error writing message {} to topic '{}': {}", header, record.topic(), mwe.getMessage());
      }
    }
  }

  @Override
  public long getOffset(TopicPartition partition) throws IllegalArgumentException {
    MongoCollection<Document> collection = healthCheckDB.getCollection(OFFSETS_COLLECTION_NAME, Document.class);
    Document offsetDoc = collection.find(
        Filters.and(
            Filters.eq(OFFSETS_TOPIC_KEY, partition.topic()),
            Filters.eq(OFFSETS_PARTITION_KEY, partition.partition())
        )
    ).first();
    if (offsetDoc != null) {
      return offsetDoc.getLong(OFFSETS_OFFSET_KEY);
    } else {
      //Place the base offset value for this topic/partition in the database to establish
      //an entry
      offsetDoc = new Document()
          .append(OFFSETS_TOPIC_KEY, partition.topic())
          .append(OFFSETS_PARTITION_KEY, partition.partition())
          .append(OFFSETS_OFFSET_KEY, 0L);
      collection.insertOne(offsetDoc);
      return 0L;
    }
  }

  @Override
  public void updateOffset(TopicPartition partition, long newOffset) throws IllegalArgumentException {
    Document newOffsetDoc = new Document()
        .append(OFFSETS_TOPIC_KEY, partition.topic())
        .append(OFFSETS_PARTITION_KEY, partition.partition())
        .append(OFFSETS_OFFSET_KEY, newOffset);

    //Only update the offset if it is the highest value possible. FindOneAndReplace is an atomic
    //update action, which will maintain thread safety
    Document updatedDoc = healthCheckDB.getCollection(OFFSETS_COLLECTION_NAME, Document.class)
        .findOneAndReplace(
            Filters.and(
                Filters.eq(OFFSETS_TOPIC_KEY, partition.topic()),
                Filters.eq(OFFSETS_PARTITION_KEY, partition.partition()),
                Filters.lt(OFFSETS_OFFSET_KEY, newOffset)
            ),
            newOffsetDoc
        );
    if (updatedDoc != null) {
      logger.debug("Set new offset to {} for topic '{}' partition {}", newOffset, partition.topic(), partition.partition());
    }
  }

  @Override
  public List<String> getTopics() {
    List<String> topicNamesList = new ArrayList<>();
    healthCheckDB.listCollections()
        .filter(Filters.ne("name", OFFSETS_COLLECTION_NAME))
        .map(document -> document.getString("name"))
        .forEach((Consumer<? super String>) topicNamesList::add);
    return topicNamesList;
  }

  @Override
  public List<String> getSystemsInTopic(String topicName) {
    List<String> systemUUIDList = new ArrayList<>();
    healthCheckDB.getCollection(topicName, HealthCheckRecord.class)
        .distinct("systemUUID", String.class)
        .forEach((Consumer<? super String>) systemUUIDList::add);
    return systemUUIDList;
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<Long> getSessionTimestamps(String topicName, String systemUUID) {
    AggregateIterable<Document> iterable = healthCheckDB.getCollection(topicName)
        .aggregate(
            Arrays.asList(
                Aggregates.match(Filters.eq("systemUUID", systemUUID)),
                Aggregates.group("$systemUUID", Accumulators.addToSet("sessionTimestamps", "$sessionTimestamp"))
            )
        );
    return iterable.first().get("sessionTimestamps", List.class);
  }

  @Override
  public List<Map<String, Object>> getSessionHealthChecks(String topicName, String systemUUID, Long sessionTimestamp) {
    List<Map<String, Object>> recordList = new ArrayList<>();
    healthCheckDB.getCollection(topicName, Document.class)
        .find(Filters.and(
            Filters.eq("systemUUID", systemUUID),
            Filters.eq("sessionTimestamp", sessionTimestamp)
        ), Document.class)
        .forEach((Consumer<? super Document>) recordList::add);
    return recordList;
  }

  // ========================== Protected Methods ==========================79
  // =========================== Private Methods ===========================79
}
