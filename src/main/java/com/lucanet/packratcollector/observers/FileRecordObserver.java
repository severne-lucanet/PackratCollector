package com.lucanet.packratcollector.observers;

import com.lucanet.packratcollector.db.DatabaseConnection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class FileRecordObserver extends AbstractRecordObserver<List<String>> {
  // =========================== Class Variables ===========================79
  // ============================ Class Methods ============================79
  // ============================   Variables    ===========================79
  // ============================  Constructors  ===========================79
  @Autowired
  public FileRecordObserver(DatabaseConnection databaseConnection) {
    super(databaseConnection);
  }

  // ============================ Public Methods ===========================79
  // ========================== Protected Methods ==========================79
  // =========================== Private Methods ===========================79
}
