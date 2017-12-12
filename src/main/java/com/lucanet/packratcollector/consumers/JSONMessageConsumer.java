package com.lucanet.packratcollector.consumers;

import com.lucanet.packratcollector.config.PackratCollectorConfig;
import com.lucanet.packratcollector.model.HealthCheckHeader;
import com.lucanet.packratcollector.model.deserializers.JSONDeserializer;
import com.lucanet.packratcollector.observers.JSONRecordObserver;
import io.reactivex.Observable;
import io.reactivex.schedulers.Schedulers;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class JSONMessageConsumer implements MessageConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(JSONMessageConsumer.class);

  private final KafkaConsumer<HealthCheckHeader, JSONObject> messageConsumer;
  private final JSONRecordObserver recordObserver;
  private final List<String> topicsList;
  private final ExecutorService threadPoolExecutor;
  private final AtomicBoolean isRunning;

  public JSONMessageConsumer(
      PackratCollectorConfig packratCollectorConfig,
      @Value("#{'${packrat.consumers.json.topics}'.split(',')}") List<String> topicsList,
      @Value("${packrat.consumers.json.threadpoolsize}") int threadPoolSize,
      JSONRecordObserver recordObserver
  ) {
    Properties props = packratCollectorConfig.generateCommonProperties();
    props.setProperty("value.deserializer", JSONDeserializer.class.getCanonicalName());
    this.messageConsumer = new KafkaConsumer<>(props);
    this.topicsList = topicsList;
    this.threadPoolExecutor = Executors.newFixedThreadPool(threadPoolSize);
    this.recordObserver = recordObserver;
    this.isRunning = new AtomicBoolean(false);
  }

  @Override
  public void run() {
    LOGGER.info("JSONMessageConsumer listening to topics {}", topicsList);
    messageConsumer.subscribe(topicsList);
    isRunning.set(true);
    while (isRunning.get()) {
      ConsumerRecords<HealthCheckHeader, JSONObject> records = messageConsumer.poll(100L);
      Observable.fromIterable(records)
          .subscribeOn(Schedulers.from(threadPoolExecutor))
          .subscribe(recordObserver);
    }
    messageConsumer.close();
  }

  @Override
  public void stop() {
    isRunning.set(false);
  }

}
