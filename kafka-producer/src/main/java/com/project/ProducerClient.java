package com.project;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerClient {
    private static final Logger logger = LoggerFactory.getLogger(ProducerClient.class);

    public static void main(String[] args) {

        logger.info("Starting Kafka Producer");

        // Create and Set Properties for Producer
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.19.146.242:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Create a Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Create a ProducerRecord
        ProducerRecord<String, String> record = new ProducerRecord<>("java", "Hello from Java Client");

        // Send the data
        producer.send(record);

        // Flush and close the producer
        producer.flush();
        producer.close();
    }
}