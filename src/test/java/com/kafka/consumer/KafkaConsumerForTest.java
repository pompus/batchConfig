package com.kafka.consumer;

import java.util.concurrent.CountDownLatch;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import lombok.extern.log4j.Log4j2;

@Log4j2
@Component
public class KafkaConsumerForTest {
	private CountDownLatch latch = new CountDownLatch(4);

	public CountDownLatch getLatch() {
		return latch;
	}

	@KafkaListener(id = "${group-id}", topics = "${kafka.topic}")
	public void receive(String payload) {
		log.debug("recieving payload {}", payload);
		latch.countDown();
	}
}
