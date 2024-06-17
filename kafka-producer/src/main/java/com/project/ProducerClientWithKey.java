package com.project;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerClientWithKey {
    private static final Logger logger = LoggerFactory.getLogger(ProducerClientWithKey.class);

    public static void main(String[] args) {

        logger.info("Starting Kafka Producer with Key");

        // Create and Set Properties for Producer
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.19.146.242:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Create a Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int j=0; j<2; j++) {

            for(int i=0; i<10; i++) {

                // Create a ProducerRecord
                String topic = "java";
                String key = "key " + i;
                String value = "value " + i;
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

                // Send the data
                producer.send(record, (recordMetadata, e) -> {
                    if(e!=null) logger.info("An Exception occurred "+e);
                    else {
                        logger.info("Topic: " + recordMetadata.topic() +
                                " | Key: " + key +
                                " | Partition: " + recordMetadata.partition());
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