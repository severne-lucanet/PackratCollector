package com.lucanet.packratcollector.consumers;

import com.lucanet.packratcollector.config.PackratCollectorConfig;
import com.lucanet.packratcollector.db.DatabaseConnection;
import com.lucanet.packratcollector.model.deserializers.JSONDeserializer;
import com.lucanet.packratcollector.observers.JSONRecordObserver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class JSONMessageConsumer extends AbstractMessageConsumer<Map<String, Object>> {

  @Autowired
  public JSONMessageConsumer(
      PackratCollectorConfig packratCollectorConfig,
      @Value("#{'${packrat.consumers.json.topics}'.split(',')}") List<String> topicsList,
      @Value("${packrat.consumers.json.threadpoolsize}") int threadPoolSize,
      JSONRecordObserver recordObserver,
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

}
