package com.kafka.e2e.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.kafka.configuration.KafkaUtils;
import com.kafka.configuration.SimpleJsonSerializer;
import com.kafka.model.KafkaModelForTest;
import com.kafka.writers.MessageSenderToKafka;

import lombok.extern.log4j.Log4j2;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes={SimpleJsonSerializer.class})
@Log4j2
public class MessageSenderToKafkaTest {
	
	private static String topicName = "testTopic";
	
	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, 1, "testTopic");
	
	@Autowired
	private SimpleJsonSerializer simpleJsonSerializer;

	@Value("${kafka.topic.key:MESSAGE_DATA}")
	private String keyName;

	@Test
	public void testSpringKafka() throws Exception {
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("boot", "false", embeddedKafka);
		Properties consumerProperties = KafkaUtils.loadConfig("consumer.properties", null);
		consumerProperties.keySet()
				.forEach(key -> consumerProps.put((String) key, consumerProperties.getOrDefault(key, "")));

		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);

		ContainerProperties containerProps = new ContainerProperties(topicName);
		final CountDownLatch latch = new CountDownLatch(4);

		containerProps.setMessageListener((AcknowledgingMessageListener<Integer, String>) (message, ack) -> {
			log.info("Receiving {}", message);
			try {
				Thread.sleep(200);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			latch.countDown();
		});

		KafkaMessageListenerContainer<Integer, String> container = new KafkaMessageListenerContainer<>(cf,
				containerProps);
		container.setBeanName("boot");

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		Properties producerProperties = KafkaUtils.loadConfig("producer.properties", null);
		producerProperties.keySet()
				.forEach(key -> senderProps.put((String) key, producerProperties.getOrDefault(key, "")));

		ProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(pf);
		kafkaTemplate.setDefaultTopic(topicName);

		container.start();

		MessageSenderToKafka<KafkaModelForTest> messageSenderToKafka = new MessageSenderToKafka<>();
		List<KafkaModelForTest> items = new ArrayList<>();

		Stream.iterate(0, element -> element + 1).limit(4).forEach(i -> items.add(new KafkaModelForTest()));
		messageSenderToKafka.setCopyAllFields(true);
		messageSenderToKafka.setSimpleJsonSerializer(simpleJsonSerializer);
		messageSenderToKafka.setTopicName(topicName);
		messageSenderToKafka.setKeyName(keyName);
		messageSenderToKafka.setKafkaTemplate(kafkaTemplate);

		messageSenderToKafka.write(items);

		kafkaTemplate.flush();

		assertThat(latch.await(20, TimeUnit.SECONDS)).isTrue();

		container.stop();
	}
}
