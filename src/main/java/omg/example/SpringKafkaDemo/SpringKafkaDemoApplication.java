package omg.example.SpringKafkaDemo;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
public class SpringKafkaDemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringKafkaDemoApplication.class, args);
	}
	/*@Bean
	public ApplicationRunner runner(KafkaTemplate<String, String> template) {
		return args -> {
			template.send("topic1", "test");
		};
	}*/


}
