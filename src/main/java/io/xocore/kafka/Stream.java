package io.xocore.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Stream {

    private static Stream instance = null;

    private List<StreamChain> streamChains = new ArrayList<StreamChain>();
    private String serverOrigin;
    private String groupId;

    private KafkaStreams streams;

    private static Stream getInstance(
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
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, CustomTimeExtractor.class);

        final StreamsBuilder builder = new StreamsBuilder();

        streamChains.forEach(streamChain -> {
            KStream kStream = builder.stream(streamChain.topicToConsume)
                    .filter((key, value) -> value != null);

            if (streamChain.consumerFilter != null) {
                kStream = kStream.filter((key, value) -> streamChain.consumerFilter.run(key, value));
            }

            if (streamChain.topicToProduce == null) {
                kStream.foreach((key, value) -> streamChain.consumerHandler.run((String)value));
            } else {
                kStream.mapValues(value -> streamChain.consumerHandler.runAndReturn((String)value))
                        .to(streamChain.topicToProduce);
            }
        });

        final Topology topology = builder.build();
        return new KafkaStreams(topology, props);
    }

    private void run() {

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

    public void add(String topicToConsume, ConsumerHandler consumerHandler, String targetToProduce) {
        streamChains.add(new StreamChain(topicToConsume, consumerHandler, targetToProduce));
    }

    public void add(String topicToConsume, ConsumerHandler consumerHandler) {
        streamChains.add(new StreamChain(topicToConsume, consumerHandler));
    }

    public class StreamChain {
        private String topicToConsume;
        private ConsumerHandler consumerHandler;
        private ConsumerFilter consumerFilter;
        private String topicToProduce = null;

        public StreamChain(String topicToConsume, ConsumerHandler consumerHandler) {
            this.topicToConsume = topicToConsume;
            this.consumerHandler = consumerHandler;
        }

        public StreamChain(String topicToConsume, ConsumerHandler consumerHandler, ConsumerFilter consumerFilter) {
            this.topicToConsume = topicToConsume;
            this.consumerHandler = consumerHandler;
            this.consumerFilter = consumerFilter;
        }

        public StreamChain(String topicToConsume, ConsumerHandler consumerHandler, String topicToProduce) {
            this.topicToConsume = topicToConsume;
            this.consumerHandler = consumerHandler;
            this.topicToProduce = topicToProduce;
        }

        public StreamChain(
                String topicToConsume,
                ConsumerHandler consumerHandler,
                ConsumerFilter consumerFilter,
                String topicToProduce
        ) {
            this.topicToConsume = topicToConsume;
            this.consumerHandler = consumerHandler;
            this.consumerFilter = consumerFilter;
            this.topicToProduce = topicToProduce;
        }
    }

    public class CustomTimeExtractor implements TimestampExtractor {
        @Override
        public long extract(final ConsumerRecord<Object, Object> record, final long previousTimestamp) {
            return System.currentTimeMillis();
        }
    }
}
