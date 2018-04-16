package io.xocore.utils;

import avro.shaded.com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public class KafkaUtils {
  /**
   * Generates Kafka Message.
   * @param sourceMessageValue  consumes from kafka, null provided if it's origin
   * @param currentMessageValue payload to push to kafka
   * @param originId            entity id if sourceMap is null
   * @param serviceName         service name
   * @param topicName           topic name
   * @param targetTopic         next topic will be
   * @return Map message
   * @throws Exception throws exception when occurs
   */
  public static Map<String, Object> generateKafkaMessage(
      Map<String, Object> sourceMessageValue,
      Map<String, Object> currentMessageValue,
      String originId,
      String serviceName,
      String topicName,
      String targetTopic) throws Exception {
    KafkaPath path = setKafkaMessagePath(
        sourceMessageValue,
        originId, serviceName, topicName, targetTopic
    );
    return createKafkaMessageWithPathAndPayload(path, currentMessageValue);
  }

  /**
   * Generates Kafka Message.
   * @param sourceMessageValue  consumes from kafka, null provided if it's origin
   * @param currentMessageValue payload to push to kafka
   * @param originId            entity id if sourceMap is null
   * @param serviceName         service name
   * @param topicName           topic name
   * @return Map message
   * @throws Exception throws exception when occurs
   */
  public static Map<String, Object> generateKafkaMessage(
      Map<String, Object> sourceMessageValue,
      Map<String, Object> currentMessageValue,
      String originId,
      String serviceName,
      String topicName) throws Exception {
    KafkaPath path = setKafkaMessagePath(
        sourceMessageValue,
        originId, serviceName, topicName, null
    );
    return createKafkaMessageWithPathAndPayload(path, currentMessageValue);
  }

  private static KafkaPath setKafkaMessagePath(
      Map<String, Object> sourceMessageValue,
      String originId,
      String serviceName,
      String topicName,
      String targetTopic) throws Exception {
    KafkaPath path;
    if (sourceMessageValue != null) {
      path = getPathFromSourceMessage(sourceMessageValue, targetTopic);
    } else {
      path = createPath(originId, targetTopic);
    }
    return addNodeToPath(path, serviceName, topicName);
  }

  private static KafkaPath addNodeToPath(KafkaPath path, String serviceName, String topicName) {
    KafkaPath.Node node = new KafkaPath.Node(serviceName, topicName);
    return path.addNode(node);
  }

  private static KafkaPath getPathFromSourceMessage(Map<String, Object> sourceMessage, String targetTopic) {
    Gson gson = new Gson();
    JsonElement jsonElement = gson.toJsonTree(sourceMessage);
    KafkaPath kafkaPath = gson.fromJson(jsonElement, KafkaPath.class);
    kafkaPath.setTargetTopic(targetTopic);
    return kafkaPath;
  }

  private static KafkaPath createPath(String originId, String targetTopic) throws Exception {
    if (originId == null) {
      throw new Exception("originId is required when sourceMessage not available.");
    }
    return new KafkaPath(originId, new ArrayList<>(), targetTopic);
  }

  private static Map<String, Object> createKafkaMessageWithPathAndPayload(
      KafkaPath path,
      Map<String, Object> currentMessageValue) {
    SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    final TimeZone utc = TimeZone.getTimeZone("UTC");
    dateFormatter.setTimeZone(utc);
    currentMessageValue.put("createdAt", dateFormatter.format(new Date()));
    return ImmutableMap.of("path", path, "data", currentMessageValue);
  }

  private static class KafkaPath {
    private String originId;
    private List<Node> nodes;
    private String targetTopic;

    public KafkaPath(String originId, List<Node> nodes) {
      this.originId = originId;
      this.nodes = nodes;
    }

    public KafkaPath(String originId, List<Node> nodes, String targetTopic) {
      this.originId = originId;
      this.nodes = nodes;
      this.targetTopic = targetTopic;
    }

    public String getOriginId() {
      return originId;
    }

    public void setOriginId(String originId) {
      this.originId = originId;
    }

    public List<Node> getNodes() {
      return nodes;
    }

    public void setNodes(List<Node> nodes) {
      this.nodes = nodes;
    }

    public String getTargetTopic() {
      return targetTopic;
    }

    public void setTargetTopic(String targetTopic) {
      this.targetTopic = targetTopic;
    }

    public KafkaPath addNode(Node node) {
      this.nodes.add(node);
      return this;
    }

    public static class Node {
      private String service;
      private String topic;

      public Node(String service, String topic) {
        this.service = service;
        this.topic = topic;
      }

      public String getService() {
        return service;
      }

      public void setService(String service) {
        this.service = service;
      }

      public String getTopic() {
        return topic;
      }

      public void setTopic(String topic) {
        this.topic = topic;
      }
    }
  }
}
