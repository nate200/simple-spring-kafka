package omg.example.SpringKafkaDemo.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Component
public class TopicsConfig {

    /*@Bean
    public NewTopic topic1() {
        return TopicBuilder.name("topic1")
                .partitions(10)
                .replicas(3)
                .compact()
                .build();
    }*/
    @Bean
    public NewTopic topic1() {
        return TopicBuilder.name("topic1")
                .partitions(10)
                .replicas(3)
                .compact()
                .configs(Map.of(
                        TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,"2",
                        TopicConfig.COMPRESSION_TYPE_CONFIG,"zstd"
                ))
                .build();
    }
    /*@Bean
    public NewTopic topic2() {
        return TopicBuilder.name("thing2")
                .partitions(10)
                .replicas(3)
                .config(TopicConfig.COMPRESSION_TYPE_CONFIG, "zstd")
                .build();
    }

    @Bean
    public NewTopic topic_assignedReplicas() {
        return TopicBuilder.name("topic_assignedReplicas")
                .assignReplicas(0, Arrays.asList(1, 2))//partition_id, broker_id in server.properties
                .assignReplicas(1, Arrays.asList(2, 3))
                .assignReplicas(2, Arrays.asList(3, 1))
                .config(TopicConfig.COMPRESSION_TYPE_CONFIG, "zstd")
                .build();
    }*/
}
