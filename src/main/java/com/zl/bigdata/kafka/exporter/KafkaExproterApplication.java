package com.zl.bigdata.kafka.exporter;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class KafkaExproterApplication {

	public static void main(String[] args) {

		SpringApplication.run(KafkaExproterApplication.class, args);

	}

}
