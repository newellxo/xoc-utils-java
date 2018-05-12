package io.xocore.kafka;

/**
 * Define the interface to create a stream filter
 */
public interface StreamFilter {
    /**
     * Entry to filter the message
     * @param topic Topic to be consumed
     * @param key
     * @param value Kafka message
     * @return Boolean to decide if or not to process the message
     */
    boolean run(String topic, Object key, Object value);
}
