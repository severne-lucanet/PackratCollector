package com.lucanet.packratcollector.db;

import com.lucanet.packratcollector.model.HealthCheckHeader;
import com.lucanet.packratcollector.model.HealthCheckRecord;
import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class MongoDatabaseConnection implements DatabaseConnection {

  private static final String OFFSETS_COLLECTION_NAME = "offsets";
  private static final String OFFSETS_TOPIC_KEY       = "topic";
  private static final String OFFSETS_PARTITION_KEY   = "partition";
  private static final String OFFSETS_OFFSET_KEY      = "offset";

  private final Logger logger = LoggerFactory.getLogger(MongoDatabaseConnection.class);
  private final MongoDatabase healthCheckDB;

  @Autowired
  public MongoDatabaseConnection(
    @Value("${packrat.persister.local.url}") String dbURL,
    @Value("${packrat.persister.local.port}") int dbPort,
    @Value("${packrat.persister.local.username}") String username,
    @Value("${packrat.persister.local.password}") String password
  ) {
    logger.info("Building CouchDB connection to {}:{}@{}:{}", username, password, dbURL, dbPort);
    MongoClientOptions.Builder clientOptionsBuilder = new MongoClientOptions.Builder();
    MongoClient mongoClient = new MongoClient(
        new ServerAddress(dbURL, dbPort),
        MongoCredential.createCredential(username, "packrat_healthcheck", password.toCharArray()),
        clientOptionsBuilder.build()
    );
    this.healthCheckDB = mongoClient.getDatabase("packrat_healthcheck");
  }

  @Override
  public void persistRecord(ConsumerRecord<HealthCheckHeader, Map<String, Object>> record) throws IllegalArgumentException {
    String collectionName = record.topic().toLowerCase();
    HealthCheckHeader header = record.key();
    HealthCheckRecord healthCheckRecord = new HealthCheckRecord(header, record.value());
    try {
      healthCheckDB.getCollection(collectionName).insertOne(healthCheckRecord);
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

}
