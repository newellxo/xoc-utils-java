package io.xocore.kafka;

public interface StreamFilter {
    boolean run(Object key, Object value);
}
