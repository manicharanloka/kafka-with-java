package com.project.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerClientWithCallback {
    private static final Logger logger = LoggerFactory.getLogger(ProducerClientWithCallback.class);

    public static void main(String[] args) {

        logger.info("Starting Kafka Producer with Callback");

        // Create and Set Properties for Producer
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.19.146.242:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batch.size", "400"); // in bytes
//        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        // Create a Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int j=0; j<10; j++) {

            for(int i=0; i<30; i++) {

                // Create a ProducerRecord
                ProducerRecord<String, String> record = new ProducerRecord<>("java", "Hello from Java Client! " + i);

                // Send the data
                producer.send(record, (recordMetadata, e) -> {
                    if(e!=null) logger.info("An Exception occurred "+e);
                    else {
                        logger.info("\nSuccessfully produced below data" +
                                "\nTopic: " + recordMetadata.topic() +
                                "\nPartition: " + recordMetadata.partition() +
                                "\nOffset: " + recordMetadata.offset() +
                                "\nTimestamp: " + recordMetadata.timestamp());
                    }
                });
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        // Flush and close the producer
        producer.flush();
        producer.close();
    }
}