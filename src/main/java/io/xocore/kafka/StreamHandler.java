package io.xocore.kafka;

public interface StreamHandler {
    void run (String message);
    String runAndReturn (String message);
}
