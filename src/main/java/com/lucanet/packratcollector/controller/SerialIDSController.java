package com.lucanet.packratcollector.controller;

import com.lucanet.packratcollector.db.DatabaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletResponse;
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

  @RequestMapping(value = "", method = RequestMethod.GET, produces = "application/json")
  public Map<String, List<String>> getSerialIDS(HttpServletResponse servletResponse) {
    return databaseConnection.getSerialIDS();
  }

  @RequestMapping(value = "/{serialID}/systems")
  public Map<String, List<String>> getSystemsForSerialID(HttpServletResponse servletResponse, @PathVariable("serialID") String serialID) {
    return databaseConnection.getSystemsForSerialID(serialID);
  }

}
