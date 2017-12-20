package com.lucanet.packratcollector.consumers;

import com.lucanet.packratcollector.config.PackratCollectorConfig;
import com.lucanet.packratcollector.db.DatabaseConnection;
import com.lucanet.packratcollector.model.deserializers.JSONDeserializer;
import com.lucanet.packratcollector.observers.RecordObserver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class JSONMessageConsumer extends AbstractMessageConsumer<Map<String, Object>> {
  // =========================== Class Variables ===========================79
  // ============================ Class Methods ============================79
  // ============================   Variables    ===========================79
  // ============================  Constructors  ===========================79
  @Autowired
  public JSONMessageConsumer(
      PackratCollectorConfig packratCollectorConfig,
      @Value("#{'${packrat.consumers.json.topics}'.split(',')}") List<String> topicsList,
      @Value("${packrat.consumers.json.threadpoolsize}") int threadPoolSize,
      @Qualifier("JSONRecordObserver") RecordObserver<Map<String, Object>> recordObserver,
      DatabaseConnection databaseConnection
  ) {
    super(
        packratCollectorConfig.generateCommonProperties(),
        JSONDeserializer.class,
        topicsList,
        threadPoolSize,
        recordObserver,
        databaseConnection
    );
  }

  // ============================ Public Methods ===========================79
  // ========================== Protected Methods ==========================79
  // =========================== Private Methods ===========================79
}
