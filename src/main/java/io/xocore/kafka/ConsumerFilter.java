package io.xocore.kafka;

public interface ConsumerFilter {
    boolean run(Object key, Object value);
}
