package com.lucanet.packratcollector;

import com.lucanet.packratcollector.consumers.MessageConsumer;
import com.lucanet.packratcollector.consumers.MessageConsumerFactory;
import com.lucanet.packratcollector.model.deserializers.FileLinesDeserializer;
import com.lucanet.packratcollector.model.deserializers.JSONDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Component
public class PackratRunner implements ApplicationRunner {
  // =========================== Class Variables ===========================79
  // ============================ Class Methods ============================79
  // ============================   Variables    ===========================79
  private final Logger                logger;
  private final List<MessageConsumer> messageConsumerList;

  // ============================  Constructors  ===========================79
  @Autowired
  public PackratRunner(
      MessageConsumerFactory messageConsumerFactory,
      @Value("#{'${packrat.consumers.json.topics}'.split(',')}") List<String> jsonTopicsList,
      @Value("${packrat.consumers.json.threadpoolsize}") int jsonThreadPoolSize,
      @Value("#{'${packrat.consumers.file.topics}'.split(',')}") List<String> fileTopicsList,
      @Value("${packrat.consumers.file.threadpoolsize}") int fileThreadPoolSize
  ) {
    logger = LoggerFactory.getLogger(PackratRunner.class);
    messageConsumerList = Arrays.asList(
        messageConsumerFactory.<Map<String, Object>>createMessageConsumer("JSONMessageConsumer", JSONDeserializer.class, jsonTopicsList, jsonThreadPoolSize),
        messageConsumerFactory.<List<String>>createMessageConsumer("FileMessageConsumer", FileLinesDeserializer.class, fileTopicsList, fileThreadPoolSize)
    );
  }

  // ============================ Public Methods ===========================79
  @Override
  public void run(ApplicationArguments args) throws Exception {
    logger.debug("Running Message Consumers...");
    messageConsumerList.forEach(MessageConsumer::run);
  }

  @PreDestroy
  public void shutdown() {
    logger.debug("Stopping Message Consumers...");
    messageConsumerList.forEach(MessageConsumer::stop);
  }

  // ========================== Protected Methods ==========================79
  // =========================== Private Methods ===========================79
}
