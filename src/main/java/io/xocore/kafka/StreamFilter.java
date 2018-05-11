package io.xocore.kafka;

public interface StreamFilter {
    boolean run(String topic, Object key, Object value);
}
