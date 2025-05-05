package com.example;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import com.example.BigQueryStreamExample;
import com.example.StreamToGCS;

public class HdfsMetadataConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "hdfs-metadata-consumer-group2");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest"); // Start from the beginning of the topic if no offsets exist

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("hdfs-metadata-topic")); // Change to your topic name

        System.out.println("Kafka Consumer is listening...");

        while (true) {
            System.out.println("Start.................");
            ConsumerRecords<String, String> records = consumer.poll(10000); // Poll for new messages
            for (ConsumerRecord<String, String> record : records) {
                
                System.out.printf("Received message: key=%s, value=%s, partition=%d, offset=%d%n",
                        record.key(), record.value(), record.partition(), record.offset());
                        HashMap<String, String> metadata = new HashMap<>();
                        metadata.put("null", record.value());
                        //BigQueryStreamExample.sendRows(metadata);
                        String toGcs = record.value();
                        StreamToGCS.sendRowsToGcs(toGcs);


            }
            System.out.println("End.................");
        }
    }
}
