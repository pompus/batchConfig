package com.kafka.e2e.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.kafka.configuration.KafkaConfiguration;
import com.kafka.configuration.SimpleJsonSerializer;
import com.kafka.consumer.KafkaConsumerForTest;
import com.kafka.model.KafkaModelForTest;
import com.kafka.writers.MessageSenderToKafka;

import lombok.extern.log4j.Log4j2;

@RunWith(SpringJUnit4ClassRunner.class)
@Log4j2
@SpringBootTest(classes={SimpleJsonSerializer.class}, properties={"bootstrap.servers=${spring.embedded.kafka.brokers}",
		"spring.kafka.consumer.auto-offset-reset=earliest",
		"kafka.topic=testTopic",
		"kafka.consumer.subscribe.topics=testTopic",
		"group.id=boot",
		"kafka.consumer.concurrency.level=1",
		"kafka.topic.key:MESSAGE_DATA"})
@ContextConfiguration(classes={KafkaConfiguration.class,MessageSenderToKafka.class,KafkaConsumerForTest.class})
@DirtiesContext
public class KafkaE2ETest {

	@Value("${kafka.topic:testTopic}")
	private String topicName;

	@Value("${kafka.topic.key:MESSAGE_DATA}")
	private String keyName;
	
	@Autowired
	private SimpleJsonSerializer simpleJsonSerializer;
	
	//KafkaEmbedded must be public 
	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, 1, "testTopic");
	
	@Autowired
	KafkaTemplate<String, String> kafkaTemplate;
	
	@Autowired
	ConsumerFactory<String,String> consumerFactory;
	
	@Autowired
	ProducerFactory<String,String> producerFactory;
	
	@Autowired
	KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String,String>> kafkaListenerContainerFactory;
	
	@Autowired
	KafkaConsumerForTest kafkaConsumerTest;
	
	@Autowired
	MessageSenderToKafka messageSenderToKafka;
	
	
	@Before
	public void validate() {
		assertNotNull(simpleJsonSerializer);
		assertNotNull(kafkaTemplate);
		assertNotNull(messageSenderToKafka);
		assertNotNull(consumerFactory);
		assertNotNull(producerFactory);
		assertNotNull(kafkaListenerContainerFactory);
		assertNotNull(kafkaConsumerTest);
	}
	
	@Test
	public void testSpringKafka() throws Exception {
		List<KafkaModelForTest> items=new ArrayList<>();
		Stream.iterate(0, element->element+1).limit(4).forEach(i-> items.add(new KafkaModelForTest())); 
		messageSenderToKafka.write(items);
		kafkaTemplate.flush();
		assertThat(kafkaConsumerTest.getLatch().await(20, TimeUnit.SECONDS)).isTrue();
	}	
}
