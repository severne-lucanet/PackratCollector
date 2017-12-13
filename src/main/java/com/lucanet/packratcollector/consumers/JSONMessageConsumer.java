package com.lucanet.packratcollector.consumers;

import com.fasterxml.jackson.databind.JsonNode;
import com.lucanet.packratcollector.config.PackratCollectorConfig;
import com.lucanet.packratcollector.model.deserializers.JSONDeserializer;
import com.lucanet.packratcollector.observers.JSONRecordObserver;
import com.lucanet.packratcollector.services.OffsetLookupService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class JSONMessageConsumer extends AbstractMessageConsumer<JsonNode> {

  @Autowired
  public JSONMessageConsumer(
      PackratCollectorConfig packratCollectorConfig,
      @Value("#{'${packrat.consumers.json.topics}'.split(',')}") List<String> topicsList,
      @Value("${packrat.consumers.json.threadpoolsize}") int threadPoolSize,
      JSONRecordObserver recordObserver,
      OffsetLookupService offsetLookupService
  ) {
    super(
        packratCollectorConfig.generateCommonProperties(),
        JSONDeserializer.class,
        topicsList,
        threadPoolSize,
        recordObserver,
        offsetLookupService
    );
  }

}
