package io.xocore.kafka;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.xocore.utils.KafkaUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class Producer {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private static Producer instance = null;
    private KafkaProducer kafkaProducer;

    private final static String keyDeserializer = "org.apache.kafka.common.serialization.StringSerializer";
    private final static String valueDeserializer = "org.apache.kafka.common.serialization.StringSerializer";
    private int produceRetries = 3;
    private String serverOrigin;
    private String serviceName;

    /**
     * Get singleton instance of producer with server origin, service name and time of retires
     * @param serverOrigin remote server origin
     * @param produceRetries time of retries if produce fails
     * @param serviceName local service name
     * @return return Producer singleton instance
     */
    public static Producer getInstance(String serverOrigin, int produceRetries, String serviceName) {
        if (instance == null) {
            instance = new Producer(serverOrigin, serviceName, produceRetries);
        }
        return instance;
    }

    /**
     *  Get singleton instance of producer with server origin and service name
     * @param serverOrigin remote server origin
     * @param serviceName local service name
     * @return return Producer singleton instance
     */
    public static Producer getInstance(String serverOrigin, String serviceName) {
        if (instance == null) {
            instance = new Producer(serverOrigin, serviceName);
        }
        return instance;
    }

    private Producer(String serverOrigin, String serviceName, int produceRetries) {
        this.serverOrigin = serverOrigin;
        this.serviceName = serviceName;
        this.produceRetries = produceRetries;
    }

    private Producer(String serverOrigin, String serviceName) {
        this.serverOrigin = serverOrigin;
        this.serviceName = serviceName;
    }

    private Callback callback = new Callback() {
        public void onCompletion(RecordMetadata metadata, Exception e) {
            if(e != null)
                e.printStackTrace();
            if (metadata == null) {
                logger.info("meta data is null");
                return;
            }
            logger.info(
                    "The record was sent to topic: " + metadata.topic()
                            + " at partition: " + metadata.partition());
        }
    };

    private KafkaProducer getKafkaProducer() {
        if (kafkaProducer == null) {
            Properties config = new Properties();
            config.put("bootstrap.servers", serverOrigin);
            config.put("acks", "all");
            config.put("retries", produceRetries);
            config.put("key.serializer", keyDeserializer);
            config.put("value.serializer", valueDeserializer);
            kafkaProducer = new KafkaProducer<String, String>(config);
        }
        return kafkaProducer;
    }

    /**
     * Pushes a message to a topic in Kafka.
     * Takes a topic, message payload and callback,
     * and pushes the message to Kafka server.
     *
     * @param  topic the topic the message will be pushed to
     * @param  data message payload in string formate to push
     */
    public void produce(String topic, String data) {
        this.produce(topic, data, this.callback);
    }

    /**
     * Pushes a message to a topic in Kafka.
     * Takes a topic, message payload and callback,
     * and pushes the message to Kafka server.
     *
     * @param  topic the topic the message will be pushed to
     * @param  data message payload in string format to push
     * @param  callback callback after push
     */
    public void produce(String topic, String data, Callback callback) {
        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>(topic, data);
        this.getKafkaProducer().send(record, callback);
        this.getKafkaProducer().flush();
    }

    /**
     * Produce a XO Core format kafka message to a topic in Kafka.
     *
     * @param  sourceMessage source message
     * @param  originId the original entity id
     * @param  data core data to push
     * @param  topic the topic message from
     * @param  targetTopic the topic to push message to
     * @throws Exception throws exception when occurs
     */
    public void produceMessage (
            Map<String, Object> sourceMessage,
            String originId,
            Map<String, Object> data,
            String topic,
            String targetTopic
    )  throws Exception {
        Map<String, Object> message = KafkaUtils.generateKafkaMessage(
                sourceMessage, data, originId, serviceName, topic, targetTopic);
        Gson gson = new GsonBuilder().create();
        String msg = gson.toJson(message);
        this.produce(topic, msg);
    }
}