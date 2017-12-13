package com.lucanet.packratcollector.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

@Service
public class OffsetLookupServiceImpl implements OffsetLookupService {

  private final Logger logger = LoggerFactory.getLogger(OffsetLookupServiceImpl.class);
  private final Map<String, Map<Integer, Long>> offsetsMap;
  private final Path offsetCachePath;
  private final ObjectMapper objectMapper;

  public OffsetLookupServiceImpl(@Value("${packrat.offsetcachepath:src/main/resources/topic_partition_offsets.txt}") String offsetCachePath) {
    this.offsetsMap = new HashMap<>();
    this.offsetCachePath = Paths.get(offsetCachePath);
    this.objectMapper = new ObjectMapper();
    try {
      if (Files.exists(this.offsetCachePath)) {
        byte[] offsetPropertiesData = Files.readAllBytes(this.offsetCachePath);
        JsonNode jsonNode = objectMapper.readValue(offsetPropertiesData, JsonNode.class);
        jsonNode.fields().forEachRemaining(topicEntry -> {
          Map<Integer, Long> partitionOffsetsMap = new HashMap<>();
          topicEntry.getValue().fields().forEachRemaining(partitionEntry ->
              partitionOffsetsMap.put(Integer.valueOf(partitionEntry.getKey()), partitionEntry.getValue().longValue())
          );
          offsetsMap.put(topicEntry.getKey(), partitionOffsetsMap);
        });
      } else {
        logger.info("Could not find offset cache file '{}' - all offsets will start at zero", this.offsetCachePath);
      }
    } catch (Exception e) {
      logger.error("Unable to load offset data from '{}': {}", this.offsetCachePath, e.getMessage());
    }
  }

  @Override
  public long getOffset(TopicPartition topicPartition) {
    long offset;
    if ((offsetsMap.containsKey(topicPartition.topic())) && (offsetsMap.get(topicPartition.topic()).containsKey(topicPartition.partition()))) {
      offset = offsetsMap.get(topicPartition.topic()).get(topicPartition.partition());
    } else {
      offset = 0L;
      saveOffset(topicPartition, offset);
    }
    return offset;
  }

  @Override
  public void saveOffset(TopicPartition topicPartition, long offset) {
    synchronized (this) {
      offsetsMap.putIfAbsent(topicPartition.topic(), new HashMap<>());
      offsetsMap.get(topicPartition.topic()).put(topicPartition.partition(), offset);
      try {
        Files.write(offsetCachePath, objectMapper.writeValueAsBytes(offsetsMap));
      } catch (Exception e) {
        e.printStackTrace();
        logger.error("Cannot save offset data to '{}': {}", offsetCachePath, e.getMessage());
      }
    }
  }

}
