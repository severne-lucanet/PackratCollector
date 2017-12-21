package com.lucanet.packratcollector.controller;

import com.lucanet.packratcollector.db.DatabaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/topics")
public class TopicController {

  private final Logger logger;
  private final DatabaseConnection databaseConnection;

  public TopicController(DatabaseConnection databaseConnection) {
    this.logger = LoggerFactory.getLogger(TopicController.class);
    this.databaseConnection = databaseConnection;
  }

  @RequestMapping(value = "", method = RequestMethod.GET, produces = "application/json")
  public List<String> getTopics(HttpServletResponse response) {
    List<String> topicsList;
    try {
      topicsList = databaseConnection.getTopics();
      response.setStatus(HttpServletResponse.SC_OK);
    } catch (Exception e) {
      logger.error("Error occurred in getTopics: {}", e.getMessage());
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      topicsList = new ArrayList<>();
    }
    return topicsList;
  }

  @RequestMapping(value = "/{topicName}/systemuuids", method = RequestMethod.GET, produces = "application/json")
  public List<String> getDistinctSystems(HttpServletResponse response, @PathVariable("topicName") String topicName) {
    List<String> distinctSystemsList;
    try {
      distinctSystemsList = databaseConnection.getSystemsInTopic(topicName);
      response.setStatus(HttpServletResponse.SC_OK);
    } catch (Exception e) {
      logger.error("Error occurred in getDistinctSystems: {} ({})", e.getMessage(), e.getClass());
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      distinctSystemsList = new ArrayList<>();
    }
    return distinctSystemsList;
  }

  @RequestMapping(value = "/{topicName}/sessions", method = RequestMethod.GET, produces = "application/json")
  public List<Long> getSessionTimestamps(
      HttpServletResponse response,
      @PathVariable("topicName") String topicName,
      @RequestParam("systemUUID") String systemUUID
  ) {
    return databaseConnection.getSessionTimestamps(topicName, systemUUID);
  }

  @RequestMapping(value = "/{topicName}/healthchecks", method = RequestMethod.GET, produces = "application/json")
  public List<Map<String, Object>> getSessionHealthChecks(
      HttpServletResponse response,
      @PathVariable("topicName") String topicName,
      @RequestParam("systemUUID") String systemUUID,
      @RequestParam("sessionTimestamp") Long sessionTimestamp
  ) {
    return databaseConnection.getSessionHealthChecks(topicName, systemUUID, sessionTimestamp);
  }

}
