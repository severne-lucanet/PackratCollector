package com.lucanet.packratcollector;

import com.lucanet.packratcollector.consumers.MessageConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class PackratRunner implements ApplicationRunner {
  // =========================== Class Variables ===========================79
  // ============================ Class Methods ============================79
  // ============================   Variables    ===========================79
  private final Logger                logger;
  private final List<MessageConsumer> messageConsumerList;
  private final ExecutorService       consumerExecutorService;

  // ============================  Constructors  ===========================79
  @Autowired
  public PackratRunner(
      @Qualifier("JSONMessageConsumer") MessageConsumer jsonMessageConsumer,
      @Qualifier("fileMessageConsumer") MessageConsumer fileMessageConsumer
  ) {
    logger = LoggerFactory.getLogger(PackratRunner.class);
    messageConsumerList = new ArrayList<>();
    messageConsumerList.add(jsonMessageConsumer);
    messageConsumerList.add(fileMessageConsumer);
    consumerExecutorService = Executors.newFixedThreadPool(messageConsumerList.size());
  }

  // ============================ Public Methods ===========================79
  @Override
  public void run(ApplicationArguments args) throws Exception {
    logger.debug("Running Message Consumers...");
    messageConsumerList.forEach(messageConsumer ->
      consumerExecutorService.submit(messageConsumer::run)
    );
  }

  @PreDestroy
  public void shutdown() {
    logger.debug("Stopping Message Consumers...");
    messageConsumerList.forEach(MessageConsumer::stop);
  }

  // ========================== Protected Methods ==========================79
  // =========================== Private Methods ===========================79
}
