package io.xocore.kafka;

public interface StreamHandler {
    void run (String topic, String message);
    String runAndReturn (String topic, String message);
}
