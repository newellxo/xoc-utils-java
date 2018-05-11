package io.xocore.kafka;

/**
 * Define the interface to create a stream handler
 */
public interface StreamHandler {
    /**
     * Entry to process the message
     * @param topic Topic to be consumed
     * @param message Kafka message payload in String
     */
    void run (String topic, String message);

    /**
     * Entry to process the message
     * @param topic Topic to be consumed
     * @param message Kafka message payload in String
     * @return Return Kafka message to produce to next topic
     */
    String runAndReturn (String topic, String message);
}
