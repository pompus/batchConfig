package com.kafka.writers;

import java.util.List;
import java.util.Map;

import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.kafka.configuration.SimpleJsonSerializer;

import lombok.Data;
import lombok.extern.log4j.Log4j2;

@Component
@Data
@Log4j2
public class MessageSenderInBatchToKafka<T> implements ItemWriter<T>, InitializingBean {

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private SimpleJsonSerializer simpleJsonSerializer;

	@Value("${kafka.topic}")
	private String topicName;

	@Value("${kafka.topic.key}")
	private String keyName;

	@Value("${kafka.internal.message:false}")
	private boolean internalMessage;

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.hasText(topicName, "topic name is not valued");
		Assert.hasText(keyName, "key name is not valued");
	}

	@Override
	public void write(List<? extends T> items) throws Exception {
		for (T t : items) {
			String json = null;
			if (internalMessage) {
				json = extractMessageData(t);
			} else {
				json = simpleJsonSerializer.toJson(t);
			}
			log.debug("writing json {} to topic {}", json, topicName);
			String jsonForError = json;
			ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, keyName, json);
			future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
				@Override
				public void onSuccess(SendResult<String, String> result) {
					log.debug("successfully send to kafka, key {} topic {}, partition {}, offset {}",
							result.getProducerRecord().key(), result.getRecordMetadata().topic(),
							result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
				}
				@Override
				public void onFailure(Throwable ex) {
					log.error("write failed for item {} , error message: {} , exception {}", jsonForError,
							ex.getMessage(), ex);
				}
			});
		}
	}

	private String extractMessageData(T t) {
		String json=null;
		if (!(t instanceof Map)) {
			log.error("item is not in expected format {}" ,t);
			throw new RuntimeException("item is not in expected format");
		}
		Map<String, Object> map=(Map)t;
		if (!map.containsKey(keyName)) {
			log.error("item {} does not have key {}" ,t, keyName);
			throw new RuntimeException("item "+t +"does not have key " + keyName);
		}
		json=simpleJsonSerializer.toJson(map.get(keyName));
		return json;
	}
}
