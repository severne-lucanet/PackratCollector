package com.lucanet.packratcollector;

import com.lucanet.packratcollector.consumers.FileMessageConsumer;
import com.lucanet.packratcollector.consumers.JSONMessageConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
public class PackratRunner implements ApplicationRunner {

  private final JSONMessageConsumer jsonMessageConsumer;
  private final FileMessageConsumer fileMessageConsumer;

  @Autowired
  public PackratRunner(JSONMessageConsumer jsonMessageConsumer, FileMessageConsumer fileMessageConsumer) {
    this.jsonMessageConsumer = jsonMessageConsumer;
    this.fileMessageConsumer = fileMessageConsumer;
  }

  @Override
  public void run(ApplicationArguments args) throws Exception {
    //jsonMessageConsumer.run();
    //fileMessageConsumer.run();
  }
}
