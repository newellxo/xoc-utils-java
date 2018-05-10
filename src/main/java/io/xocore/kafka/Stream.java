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

    public static Stream getInstance(
            String serverOrigin,
            String groupId
    ) {
        if (instance == null) {
            instance = new Stream(serverOrigin, groupId);
        }
        return instance;
    }

    public Stream(
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
                    filteredStream = kStream.filter((key, value) -> consumerChain.streamFilter.run(key, value));
                }

                if (consumerChain.topicToProduce == null) {
                    filteredStream.foreach((key, value) -> consumerChain.streamHandler.run((String)value));
                } else {
                    filteredStream.mapValues(value -> consumerChain.streamHandler.runAndReturn((String)value))
                            .to(consumerChain.topicToProduce);
                }
            }
        });

        final Topology topology = builder.build();
        return new KafkaStreams(topology, props);
    }

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

    public void add(String topicToConsume, StreamHandler streamHandler, String topicToProduce) {
        saveToStreamChains(topicToConsume, new ConsumerChain(streamHandler, topicToProduce));
    }

    public void add(String topicToConsume, StreamHandler streamHandler, String topicToProduce, StreamFilter streamFilter) {
        saveToStreamChains(topicToConsume, new ConsumerChain(streamHandler, topicToProduce, streamFilter));
    }

    public void add(String topicToConsume, StreamHandler streamHandler) {
        saveToStreamChains(topicToConsume, new ConsumerChain(streamHandler));
    }

    public void add(String topicToConsume, StreamHandler streamHandler, StreamFilter streamFilter) {
        saveToStreamChains(topicToConsume, new ConsumerChain(streamHandler, streamFilter));
    }

    public class ConsumerChain {
        private StreamHandler streamHandler;
        private StreamFilter streamFilter;
        private String topicToProduce = null;

        public ConsumerChain(StreamHandler streamHandler) {
            this.streamHandler = streamHandler;
        }

        public ConsumerChain(StreamHandler streamHandler, StreamFilter streamFilter) {
            this.streamHandler = streamHandler;
            this.streamFilter = streamFilter;
        }

        public ConsumerChain(StreamHandler streamHandler, String topicToProduce) {
            this.streamHandler = streamHandler;
            this.topicToProduce = topicToProduce;
        }

        public ConsumerChain(
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