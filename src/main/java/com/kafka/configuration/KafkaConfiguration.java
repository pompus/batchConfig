package com.kafka.configuration;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import lombok.extern.log4j.Log4j2;

@EnableKafka
@Configuration
@Log4j2
public class KafkaConfiguration implements InitializingBean {

	@Value("${bootstrap.servers}")
	private String kafkaServer;

	@Value("${kafka.keystore:#{null}}")
	private String keyStoreFile;

	@Value("${kafka.truststore:#{null}}")
	private String trustStoreFile;

	@Value("${kafka.ssl.truststore.password:#{null}}")
	private String sslTrustStorePassword;

	@Value("${kafka.ssl.keystore.password:#{null}}")
	private String keyStorePassword;

	@Value("${kafka.security.protocol:#{null}}")
	private String securityProtocol;

	@Value("${kafka.ssl.key.password:#{null}}")
	private String sslkeyPassword;

	@Value("${kafka.consumer.concurrency.level:3}")
	private int concurrencylevel;

	@Value("${group.id:#{null}}")
	private String consumerGroupId;

	@Value("${kafka.consumer.subscribe.topics:#{null}}")
	private String topicNamesToSubScribe;

	@Value("${kafka.consumer.poll.timeout:3000}")
	private int pollTimeOut;

	@Value("${kafka.producer.file.config:#{null}}")
	private String producerFileConfig;

	@Value("${kafka.consumer.file.config:#{null}}")
	private String consumerFileConfig;

	@Bean
	public KafkaTemplate<String, String> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}

	@Bean
	public ProducerFactory<String, String> producerFactory() {
		Map<String, Object> config = config("producer.properties", producerFileConfig);
		return new DefaultKafkaProducerFactory<>(config);
	}
	
	@Bean
	KafkaConsumer<String, String> kafkaConsumer(){
		Map<String, Object> config = config("consumer.properties", consumerFileConfig);
		log.debug("kafka consumer properties loaded {}", config);
		if (!StringUtils.isEmpty(consumerGroupId)) {
			config.put("group.id", consumerGroupId);
		} else if (!config.containsKey("group.id")) {
			log.warn("please set unique group.id for consumer in properties");
		}
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(config);
		if (topicNamesToSubScribe == null) {
			log.warn("please set kafka.consumer.subscribe.topics with comma separated topic name in your property");
		} else {
			kafkaConsumer.subscribe(topicName());
		}
		return kafkaConsumer;
	}

	@Bean
	public ConsumerFactory<String, String> consumerFactory() {
		Map<String, Object> config = config("consumer.properties", consumerFileConfig);
		log.debug("kafka consumer properties loaded {}", config);
		if (!StringUtils.isEmpty(consumerGroupId)) {
			config.put("group.id", consumerGroupId);
		} else if (!config.containsKey("group.id")) {
			log.warn("please set unique group.id for consumer in properties");
		}
		return new DefaultKafkaConsumerFactory<>(config);
	}

	@Bean
	KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory());
		factory.setConcurrency(concurrencylevel);
		factory.getContainerProperties().setPollTimeout(pollTimeOut);
		factory.getContainerProperties().setIdleEventInterval(1000L);
		return factory;
	}

	@Bean
	KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> batchKafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory());
		factory.setConcurrency(concurrencylevel);
		factory.getContainerProperties().setPollTimeout(pollTimeOut);
		factory.getContainerProperties().setIdleEventInterval(1000L);
		factory.setBatchListener(true);
		factory.getContainerProperties().setAckMode(AbstractMessageListenerContainer.AckMode.BATCH);
		return factory;
	}

	private Collection<String> topicName() {
		if (topicNamesToSubScribe.indexOf(",") == -1) {
			return Collections.singletonList(topicNamesToSubScribe);
		}
		return Arrays.asList(topicNamesToSubScribe.split(","));
	}

	private Map<String, Object> config(final String defaultConfigFile, final String overridenConfigFile) {
		Map<String, Object> config = new HashMap<>();
		Properties properties = KafkaUtils.loadConfig(defaultConfigFile, overridenConfigFile);

		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
		if (securityProtocol != null) {
			properties.put("ssl.truststore.password", sslTrustStorePassword);
			properties.put("ssl.keystore.location", trustStoreFile);
			properties.put("security.protocol", securityProtocol);
			properties.put("ssl.truststore.location", keyStoreFile);
			properties.put("ssl.keystore.password", keyStorePassword);
			properties.put("ssl.key.password", sslkeyPassword);
		}

		properties.keySet().forEach(key -> config.put((String) key, properties.getOrDefault(key, "")));
		return config;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.hasText(kafkaServer, "please set bootstrap.servers in the confiration");
	}
}
