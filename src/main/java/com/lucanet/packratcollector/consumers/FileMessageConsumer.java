package com.lucanet.packratcollector.consumers;

import com.lucanet.packratcollector.config.PackratCollectorConfig;
import com.lucanet.packratcollector.observers.FileRecordObserver;
import com.lucanet.packratcollector.services.OffsetLookupService;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class FileMessageConsumer extends AbstractMessageConsumer<byte[]> {

  @Autowired
  public FileMessageConsumer(
      PackratCollectorConfig packratCollectorConfig,
      @Value("#{'${packrat.consumers.file.topics}'.split(',')}") List<String> topicsList,
      @Value("${packrat.consumers.file.threadpoolsize}") int threadPoolSize,
      FileRecordObserver fileRecordObserver,
      OffsetLookupService offsetLookupService
  ) {
    super(
        packratCollectorConfig.generateCommonProperties(),
        ByteArrayDeserializer.class,
        topicsList,
        threadPoolSize,
        fileRecordObserver,
        offsetLookupService
    );
  }

}
