package io.xocore.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Stream {

    private static Stream instance = null;

    private HashMap<String, List<ConsumerChain>> streamChains = new HashMap<>();
    private String serverOrigin;
    private String groupId;

    private KafkaStreams streams;

    /**
     * Singleton instance of stream
     * @param serverOrigin Kafka server origin
     * @param groupId Kafka consumer group id
     * @return Stream singleton instance
     */
    public static Stream getInstance(
            String serverOrigin,
            String groupId
    ) {
        if (instance == null) {
            instance = new Stream(serverOrigin, groupId);
        }
        return instance;
    }

    private Stream(
            String serverOrigin,
            String groupId
    ) {
        this.serverOrigin = serverOrigin;
        this.groupId = groupId;
    }

    private KafkaStreams initStreams() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, groupId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, serverOrigin);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, StreamTimeExtractor.class);

        final StreamsBuilder builder = new StreamsBuilder();

        streamChains.forEach((topic, consumerChains) -> {
            KStream kStream = builder.stream(topic)
                    .filter((key, value) -> value != null);

            for (ConsumerChain consumerChain : consumerChains) {
                KStream filteredStream = kStream;
                if (consumerChain.streamFilter != null) {
                    filteredStream = kStream.filter((key, value) -> consumerChain.streamFilter.run(topic, key, value));
                }

                if (consumerChain.topicToProduce == null) {
                    filteredStream.foreach((key, value) -> consumerChain.streamHandler.run(topic, (String)value));
                } else {
                    filteredStream.mapValues(value -> consumerChain.streamHandler.runAndReturn(topic, (String)value))
                            .to(consumerChain.topicToProduce);
                }
            }
        });

        final Topology topology = builder.build();
        return new KafkaStreams(topology, props);
    }

    /**
     * Start streams
     */
    public void run() {
        streams = initStreams();

        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook"){
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    private void saveToStreamChains(String topicToConsume, ConsumerChain consumerChain) {
        if (streamChains.containsKey(topicToConsume)) {
            streamChains.get(topicToConsume).add(consumerChain);
        } else {
            List<ConsumerChain> consumerChains = new ArrayList<>();
            consumerChains.add(consumerChain);
            streamChains.put(topicToConsume, consumerChains);
        }
    }

    /**
     * Add a stream chain with topic to be consumed, handler and topic to produced.
     * @param topicToConsume Kafka topic to be consumed
     * @param streamHandler Handler to consume the topic message
     * @param topicToProduce Kafka topic to produce to after handler
     */
    public void add(String topicToConsume, StreamHandler streamHandler, String topicToProduce) {
        saveToStreamChains(topicToConsume, new ConsumerChain(streamHandler, topicToProduce));
    }

    /**
     * Add a stream chain with topic to be consumed, handler, topic to produced and message filter.
     * @param topicToConsume Kafka topic to be consumed
     * @param streamHandler Handler to consume the topic message
     * @param topicToProduce Kafka topic to produce to after handler
     * @param streamFilter Filter the message before going to handler
     */
    public void add(String topicToConsume, StreamHandler streamHandler, String topicToProduce, StreamFilter streamFilter) {
        saveToStreamChains(topicToConsume, new ConsumerChain(streamHandler, topicToProduce, streamFilter));
    }

    /**
     * Add a stream chain with topic to be consumed and handler.
     * @param topicToConsume Kafka topic to be consumed
     * @param streamHandler Handler to consume the topic message
     */
    public void add(String topicToConsume, StreamHandler streamHandler) {
        saveToStreamChains(topicToConsume, new ConsumerChain(streamHandler));
    }

    /**
     * Add a stream chain with topic to be consumed, handler and message filter.
     * @param topicToConsume Kafka topic to be consumed
     * @param streamHandler Handler to consume the topic message
     * @param streamFilter Filter the message before going to handler
     */
    public void add(String topicToConsume, StreamHandler streamHandler, StreamFilter streamFilter) {
        saveToStreamChains(topicToConsume, new ConsumerChain(streamHandler, streamFilter));
    }

    private class ConsumerChain {
        private StreamHandler streamHandler;
        private StreamFilter streamFilter;
        private String topicToProduce = null;

        private ConsumerChain(StreamHandler streamHandler) {
            this.streamHandler = streamHandler;
        }

        private ConsumerChain(StreamHandler streamHandler, StreamFilter streamFilter) {
            this.streamHandler = streamHandler;
            this.streamFilter = streamFilter;
        }

        private ConsumerChain(StreamHandler streamHandler, String topicToProduce) {
            this.streamHandler = streamHandler;
            this.topicToProduce = topicToProduce;
        }

        private ConsumerChain(
                StreamHandler streamHandler,
                String topicToProduce,
                StreamFilter streamFilter
        ) {
            this.streamHandler = streamHandler;
            this.topicToProduce = topicToProduce;
            this.streamFilter = streamFilter;
        }
    }
}