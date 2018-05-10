package io.xocore.kafka;

import org.codehaus.jackson.JsonNode;

public interface ConsumerHandler {
    void run (JsonNode message) throws Exception;
}
