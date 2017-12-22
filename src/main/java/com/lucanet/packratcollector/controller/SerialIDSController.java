package com.lucanet.packratcollector.controller;

import com.lucanet.packratcollector.aspects.LogExecution;
import com.lucanet.packratcollector.db.DatabaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/serialIDS")
public class SerialIDSController {

  private final Logger logger;
  private final DatabaseConnection databaseConnection;

  public SerialIDSController(DatabaseConnection databaseConnection) {
    this.logger = LoggerFactory.getLogger(SerialIDSController.class);
    this.databaseConnection = databaseConnection;
  }

  @LogExecution
  @RequestMapping(value = "", method = RequestMethod.GET, produces = "application/json")
  public Map<String, List<String>> getSerialIDS(HttpServletResponse servletResponse) {
    Map<String, List<String>> serialIDS = new HashMap<>();
    try {
      serialIDS.putAll(databaseConnection.getSerialIDS());
    } catch (Exception e) {
      logger.error("Error occurred in getSerialIDS: {} ({})", e.getMessage(), e.getClass());
      servletResponse.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
    return serialIDS;
  }

  @LogExecution
  @RequestMapping(value = "/{serialID}/systems")
  public Map<String, List<String>> getSystemsForSerialID(HttpServletResponse servletResponse, @PathVariable("serialID") String serialID) {
    Map<String, List<String>> serialIDSystems = new HashMap<>();
    try {
      serialIDSystems.putAll(databaseConnection.getSystemsForSerialID(serialID));
    } catch (Exception e) {
      logger.error("Error occurred in getSystemsForSerialID: {} ({})", e.getMessage(), e.getClass());
      servletResponse.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
    return serialIDSystems;
  }

}
