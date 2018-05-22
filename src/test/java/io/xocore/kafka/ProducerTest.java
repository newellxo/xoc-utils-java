package io.xocore.kafka;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.curator.test.TestingServer;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ProducerTest {

    private static TestingServer testingServer;
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

        producer = Producer.getInstance("localhost:9092", "xoc-utils");
    }

    @Test
    public void testProduce() {
        producer.produce("xoc-utils-testing", "this is a testing");
    }

    @Test
    public void testProduceMessage() {
        String originId = "iamoriginid";
        Map<String, Object> data = new HashMap<>();
        data.put("message", "testing");
        String topic = "xoc-utils-testing";
        String targetTopic = "xoc-utils-targetTopic";
        try {
            producer.produceMessage(null, originId, data, topic, targetTopic);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
