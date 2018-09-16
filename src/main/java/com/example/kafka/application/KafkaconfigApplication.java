package com.example.kafka.application;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@ComponentScan("basePackages=com.kafka")
@SpringBootApplication
public class KafkaconfigApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaconfigApplication.class, args);
	}
}
