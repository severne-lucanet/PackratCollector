package com.lucanet.packratcollector;

import com.lucanet.packratcollector.consumers.FileMessageConsumer;
import com.lucanet.packratcollector.consumers.JSONMessageConsumer;
import com.lucanet.packratcollector.consumers.MessageConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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

  private static final Logger LOGGER = LoggerFactory.getLogger(PackratRunner.class);

  private final List<MessageConsumer> messageConsumerList;
  private final ExecutorService consumerExecutorService;

  @Autowired
  public PackratRunner(JSONMessageConsumer jsonMessageConsumer, FileMessageConsumer fileMessageConsumer) {
    messageConsumerList = new ArrayList<>();
    messageConsumerList.add(jsonMessageConsumer);
    messageConsumerList.add(fileMessageConsumer);
    consumerExecutorService = Executors.newFixedThreadPool(messageConsumerList.size());
  }

  @Override
  public void run(ApplicationArguments args) throws Exception {
    LOGGER.debug("Running Message Consumers...");
    messageConsumerList.forEach(messageConsumer ->
      consumerExecutorService.submit(messageConsumer::run)
    );
  }

  @PreDestroy
  public void shutdown() {
    LOGGER.debug("Stopping Message Consumers...");
    messageConsumerList.forEach(MessageConsumer::stop);
  }
}
