package io.xocore.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Consumer {

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private ObjectMapper mapper = new ObjectMapper();
    private SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

    private String serviceName;
    private String serverOrigin;
    private  String groupId;
    private final static String keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
    private final static String valueDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
    private String dealLetterTopic = "xoc.dead-letter-queue";
    private int pollTimeout = 3000;
    private boolean autoCommitEnable = true;
    private int autoCommitInterval = 1000;
    private int consumerRetries = 0;
    private Producer producer;

    private static Consumer instance = null;

    private KafkaConsumer<String, String> kafkaConsumer;

    private HashMap<String, ConsumerHandler> consumerHandlers = new HashMap<>();

    /**
     * Get a singleton consumer instance with server origin, group ID and local service name.
     * @param serverOrigin remote server origin
     * @param groupId consumer group ID
     * @param serviceName local service name
     * @return return Consumer singleton instance
     */
    public static Consumer getInstance(
            String serverOrigin,
            String groupId,
            String serviceName
    ) {
        if (instance == null) {
            instance  = new Consumer(
                    serverOrigin,
                    groupId,
                    serviceName
            );
        }
        return instance;
    }

    /**
     * Get a singleton consumer instance with configurations.
     * @param serverOrigin remote server origin
     * @param groupId consumer group ID
     * @param autoCommitEnable if enable auto commit
     * @param autoCommitInterval auto commit interval
     * @param pollTimeout time period to poll message from server
     * @param dealLetterTopic dead letter topic
     * @param serviceName local service name
     * @param consumerRetries time of retires if consume fails
     * @return return Consumer singleton instance
     */
    public static Consumer getInstance(
            String serverOrigin,
            String groupId,
            boolean autoCommitEnable,
            int autoCommitInterval,
            int pollTimeout,
            String dealLetterTopic,
            String serviceName,
            int consumerRetries
    ) {
        if (instance == null) {
            instance  = new Consumer(
                    serverOrigin,
                    groupId,
                    autoCommitEnable,
                    autoCommitInterval,
                    pollTimeout,
                    dealLetterTopic,
                    serviceName,
                    consumerRetries);
        }
        return instance;
    }

    private Consumer(
            String serverOrigin,
            String groupId,
            String serviceName
    ) {
        this.serverOrigin = serverOrigin;
        this.groupId = groupId;
        this.serviceName = serviceName;
        this.producer = Producer.getInstance(serverOrigin, serviceName);
    }

    private Consumer(
            String serverOrigin,
            String groupId,
            boolean autoCommitEnable,
            int autoCommitInterval,
            int pollTimeout,
            String dealLetterTopic,
            String serviceName,
            int consumerRetries
    ) {
        this.serverOrigin = serverOrigin;
        this.groupId = groupId;
        this.autoCommitEnable = autoCommitEnable;
        this.autoCommitInterval = autoCommitInterval;
        this.pollTimeout = pollTimeout;
        this.dealLetterTopic = dealLetterTopic;
        this.serviceName = serviceName;
        this.consumerRetries = consumerRetries;
        this.producer = Producer.getInstance(serverOrigin, serviceName);
    }

    private KafkaConsumer getKafkaConsumer() {
        if(kafkaConsumer == null) {
            Properties props = new Properties();
            props.put("bootstrap.servers", serverOrigin);
            props.put("group.id", groupId);
            props.put("enable.auto.commit", autoCommitEnable);
            props.put("auto.commit.interval.ms", autoCommitInterval);
            props.put("key.deserializer", keyDeserializer);
            props.put("value.deserializer", valueDeserializer);
            kafkaConsumer = new KafkaConsumer<>(props);
        }
        return kafkaConsumer;
    }

    private ObjectNode createErrorNode(Exception ex) {
        ObjectNode errorNode = mapper.createObjectNode();
        errorNode.put("exception", Arrays.toString(ex.getStackTrace()));
        errorNode.put("@timestamp", dateFormatter.format((new Date()).getTime()));

        return errorNode;
    }

    private ObjectNode createRetry(Exception ex, String originTopic) {
        ArrayNode errors = mapper.createArrayNode();
        errors.add(createErrorNode(ex));

        ObjectNode retryObj = mapper.createObjectNode();
        retryObj.put("service", this.serviceName);
        retryObj.put("topic", originTopic);
        retryObj.put("errors", errors);

        return retryObj;
    }

    private void handleConsumerException(
            Exception ex,
            ConsumerRecord<String, String> record,
            String targetTopic
    ) {
        ObjectNode msgObj = mapper.createObjectNode();
        msgObj.put("message", record.value());
        msgObj.put("retry", createRetry(ex, record.topic()));
        this.producer.produce(targetTopic, msgObj.toString());
    }

    private void handleConsumerException(
            Exception ex,
            ObjectNode recordNode,
            String targetTopic,
            String originTopic
    ) {
        if (recordNode.has("retry")) { // if it is retry, add to errors
            ArrayNode errors = (ArrayNode) recordNode.get("retry").get("errors");
            if (errors.size() > consumerRetries) {
                targetTopic = dealLetterTopic;
                this.producer.produce(targetTopic, recordNode.toString());
            } else {
                errors.add(createErrorNode(ex));
                this.producer.produce(targetTopic, recordNode.toString());
            }
        } else { // wrap message with retry
            ObjectNode newRecordNode = mapper.createObjectNode();
            newRecordNode.put("message", recordNode);
            newRecordNode.put("retry", createRetry(ex, originTopic));
            this.producer.produce(targetTopic, newRecordNode.toString());
        }
    }

    /**
     * Adds handler for topic.
     * @param topic the topic of the message to handle
     * @param consumerhandler handler instance that have to implement the interface.
     */
    public void addConsumerHandler(String topic, ConsumerHandler consumerhandler) {
        consumerHandlers.put(topic, consumerhandler);
    }

    /**
     * Starts to consume Kafka message, throws exceptions if no handler added.
     * @throws Exception throws exception when occurs
     */
    public void consume() throws Exception {
        if (this.consumerHandlers.isEmpty()) {
            logger.error("There are registered topics.");
            throw new Exception("There are no registered topics.");
        }

        List<String> topicList = new ArrayList<>(this.consumerHandlers.keySet());
        this.getKafkaConsumer().subscribe(topicList);

        while (true) {
            ConsumerRecords<String, String> records = this.getKafkaConsumer().poll(pollTimeout);
            for (ConsumerRecord<String, String> record : records) {
                logger.info("topic = {}, offset = {}, key = {}, value = {}",
                        record.topic(), record.offset(), record.key(), record.value());
                JsonNode recordNode;
                String targetTopic = record.topic() + "-retry";;
                try {
                    recordNode = mapper.readTree(record.value());
                } catch (IOException ex) {
                    handleConsumerException(ex, record, this.dealLetterTopic);
                    continue;
                }
                try {
                    String originTopic = record.topic();
                    JsonNode msgNode = recordNode;
                    if (recordNode.has("message")) { // if it is retry topic
                        msgNode = recordNode.get("message");
                        targetTopic = record.topic();
                        originTopic = recordNode.get("retry").get("topic").asText();
                    }
                    consumerHandlers.get(originTopic).run(msgNode);
                } catch (Exception ex) {
                    ex.printStackTrace();
                    if (consumerRetries > 0) {
                        handleConsumerException(ex, (ObjectNode)recordNode, targetTopic, record.topic());
                    }
                }
            }
        }
    }
}
