package io.xocore.examples;

import io.xocore.kafka.Consumer;
import io.xocore.kafka.ConsumerHandler;
import org.codehaus.jackson.JsonNode;

import java.util.concurrent.CountDownLatch;

public class ConsumerExample {

    public static class ConsumerThread extends Thread {

        public void run() {
            Consumer consumer = Consumer.getInstance(
                    "mb.xocore-sandbox.io:9092",
                    "your_group_id",
                    true,
                    1000,
                    3000,
                    "dead-letter-queue",
                    "service_name",
                    2
            );

            final CountDownLatch latch = new CountDownLatch(1);
            Runtime.getRuntime().addShutdownHook(new Thread("shutdown") {
                public void run() {
                    consumer.stop();
                    try {
                        latch.await();
                    } catch (Throwable e) {
                        System.exit(1);
                    }
                    System.out.println("shutting down");
                }
            });

            consumer.addConsumerHandler("hello", new Handler());
            try {
                consumer.consume();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            latch.countDown();
        }
    }

    public static class Handler implements ConsumerHandler {
        @Override
        public void run(JsonNode message) throws Exception {
            System.out.println(message);
        }
    }

    public static void main(String[] args) throws Exception {
        (new ConsumerThread()).run();
    }
}
