package io.xocore.kafka;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.curator.test.TestingServer;

import org.codehaus.jackson.JsonNode;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Properties;

public class ConsumerTest {

    private static TestingServer testingServer;
    private static Consumer consumer;
    private static Producer producer;

    @BeforeClass
    public static void setUp() throws Exception {
        testingServer = new TestingServer(2181);

        Properties props = new Properties();
        props.put("broker.id", "0");
        props.put("host.name", "localhost");
        props.put("port", "9092");
        props.put("log.dir", "/tmp/tmp_kafka_dir");
        props.put("zookeeper.connect", testingServer.getConnectString());
        props.put("replica.socket.timeout.ms", "1500");
        KafkaConfig config = new KafkaConfig(props);

        KafkaServerStartable kafkaServerStartable = new KafkaServerStartable(config);
        kafkaServerStartable.startup();

        consumer = Consumer.getInstance(
                "localhost:9092", "xoc-utils-group","xoc-utils");
    }

    public class Handler implements ConsumerHandler {

        @Override
        public void run(JsonNode message) throws Exception {
            System.out.println(message.toString());
        }
    }

    @Test
    public void testAddConsumerHandler() {
        consumer.addConsumerHandler("xoc-utils-testing", new Handler());

        try {
            consumer.consume();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }
}
