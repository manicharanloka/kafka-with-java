package com.project.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerClientWithShutdown {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerClientWithShutdown.class);

    public static void main(String[] args) {

        logger.info("Starting Kafka Consumer with Shutdown");

        String groupId = "JavaGroup";
        String topic = "java";

        // Create and Set Properties for Consumer
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.19.146.242:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id",groupId);
        properties.setProperty("auto.offset.reset","earliest");

        // Create a Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Get a reference to main thread
        final Thread main = Thread.currentThread();

        // Add Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Detected a shutdown, calling consumer.wakeup()");
            consumer.wakeup();
            try {
                main.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            logger.info("Exiting child thread");
        }));

        try {
            // Subscribe to a topic
            consumer.subscribe(List.of(topic));

            // Poll for data
            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for(ConsumerRecord<String, String> record: records) {
                    logger.info("Topic: " + record.topic() + ", Partition: " + record.partition() + ", Offset: "
                            + record.offset() + ", Key: " + record.key() + ", Value: " + record.value());
                }
            }
        }catch (WakeupException e) {
            logger.info("Gracefully shutting down");
        }catch (Exception e) {
            logger.info("Encountered an exception");
        }finally {
            // Close the consumer which also commits offset
            consumer.close();
            logger.info("Consumer is now closed");
        }
    }
}