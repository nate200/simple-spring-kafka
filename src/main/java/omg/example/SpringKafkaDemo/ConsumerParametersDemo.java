package omg.example.SpringKafkaDemo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;

import org.springframework.kafka.listener.adapter.ConsumerRecordMetadata;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class ConsumerParametersDemo {
    @KafkaListener(id = "myId1", topics = "spring-kafka-topic-listen")
    public void listen(String in) {
        System.out.println(in);
    }
    @KafkaListener(id = "myId2", topics = "spring-kafka-topic-listen_annotation")
    public void listen_annotation(@Payload String val,
                       @Header(KafkaHeaders.RECEIVED_KEY) Integer key,
                       @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                       @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                       @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long ts,
                      @Header(KafkaHeaders.OFFSET) long offset) {
        System.out.println("topic:{" + topic + ":" + partition + ":" + offset + "}, msg: {"+key + ":" + val+"}, ts:" + ts);
    }
    @KafkaListener(id = "myId2_1", topics = "spring-kafka-topic-listen_ConsumerRecordMetadata")
    public void listen_annotation(String val, ConsumerRecordMetadata meta) {
        System.out.println("topic:{" + meta.topic() + ":" + meta.partition() + ":" + meta.offset() + "}, msg: {"+val+"}, ts:" + meta.timestamp());
    }
    @KafkaListener(id = "myId3", topics = "spring-kafka-topic-listener_ConsumerRecord")
    void listener_ConsumerRecord(ConsumerRecord<String, String> record) {//doesn't work with setBatchListener(true)
        System.out.println("topic:{" + record.topic() + ":" + record.partition() + ":" + record.offset() +
                "}, msg: {"+record.key() + ":" + record.value()+
                "}, ts:" + record.timestamp());
    }

    @KafkaListener(id = "myId4", topics = "spring-kafka-topic-batchListener", containerFactory = "batchFactory")
    void batchListener(List<String> in) {
        System.out.println(in);
    }
    @KafkaListener(id = "myId5", topics = "spring-kafka-topic-batchListen_annotation", containerFactory = "batchFactory")
    void batchListen_annotation(@Payload List<String> vals,
                                @Header(KafkaHeaders.RECEIVED_KEY) List<Integer> keys,
                                @Header(KafkaHeaders.RECEIVED_TOPIC) List<String> topics,
                                @Header(KafkaHeaders.RECEIVED_PARTITION) List<Integer> partitions,
                                @Header(KafkaHeaders.RECEIVED_TIMESTAMP) List<Long> ts,
                                @Header(KafkaHeaders.OFFSET) List<Long> offsets)
    {
        System.out.println("batchListen_annotation start");
        for(int i=0;i<vals.size();i++) {
            System.out.println("topic:{" + topics.get(i) + ":" + partitions.get(i) + ":" + offsets.get(i) +
                    "}, msg: {" + keys.get(i) + ":" + vals.get(i) +
                    "}, ts:" + ts.get(i));
        }
        System.out.println("batchListen_annotation end");
    }

    @KafkaListener(id = "myId6", topics = "spring-kafka-topic-batchListener_ConsumerRecord", containerFactory = "batchFactory")
    void batchListener_ConsumerRecord(List<ConsumerRecord<String, String>> records) {
        System.out.println("batchListener_ConsumerRecord start");
        for (ConsumerRecord<String, String> record : records) {
            System.out.println("topic:{" + record.topic() + ":" + record.partition() + ":" + record.offset() +
                    "}, msg: {"+record.key() + ":" + record.value()+
                    "}, ts:" + record.timestamp());
        }
        System.out.println("batchListener_ConsumerRecord end");
    }
    @KafkaListener(id = "myId7", topics = "spring-kafka-topic-batchListener_ConsumerRecords", containerFactory = "batchFactory")
    void batchListener_ConsumerRecords(ConsumerRecords<String, String> records) {//doesn't work with setBatchListener(false)
        System.out.println("batchListener_ConsumerRecords start");
        for (TopicPartition topic : records.partitions()) {
            for (ConsumerRecord<String, String> record : records.records(topic)) {
                records.records(topic);
                System.out.println("topic:{" + record.topic() + ":" + record.partition() + ":" + record.offset() +
                        "}, msg: {"+record.key() + ":" + record.value()+
                        "}, ts:" + record.timestamp());
            }
            System.out.println("------------------");
        }
        System.out.println("batchListener_ConsumerRecords end");
    }
}
