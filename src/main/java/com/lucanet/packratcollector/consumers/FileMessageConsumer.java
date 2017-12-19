package com.lucanet.packratcollector.consumers;

import com.lucanet.packratcollector.config.PackratCollectorConfig;
import com.lucanet.packratcollector.db.DatabaseConnection;
import com.lucanet.packratcollector.observers.FileRecordObserver;
import com.lucanet.packratcollector.observers.RecordObserver;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class FileMessageConsumer extends AbstractMessageConsumer<byte[]> {
  // =========================== Class Variables ===========================79
  // ============================ Class Methods ============================79
  // ============================   Variables    ===========================79
  // ============================  Constructors  ===========================79
  @Autowired
  public FileMessageConsumer(
      PackratCollectorConfig packratCollectorConfig,
      @Value("#{'${packrat.consumers.file.topics}'.split(',')}") List<String> topicsList,
      @Value("${packrat.consumers.file.threadpoolsize}") int threadPoolSize,
      @Qualifier("fileRecordObserver") RecordObserver<byte[]> fileRecordObserver,
      DatabaseConnection databaseConnection
  ) {
    super(
        packratCollectorConfig.generateCommonProperties(),
        ByteArrayDeserializer.class,
        topicsList,
        threadPoolSize,
        fileRecordObserver,
        databaseConnection
    );
  }

  // ============================ Public Methods ===========================79
  // ========================== Protected Methods ==========================79
  // =========================== Private Methods ===========================79
}
