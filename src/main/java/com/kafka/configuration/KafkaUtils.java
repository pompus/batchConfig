package com.kafka.configuration;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.springframework.core.io.ClassPathResource;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

public class KafkaUtils {

	public static Properties loadConfig(String defaultConfigFile, String overridenConfigFile) {
		Properties properties = new Properties();
		try {
			properties.load(new ClassPathResource(defaultConfigFile).getInputStream());
		} catch (IOException e) {
			throw new RuntimeException(defaultConfigFile + "is not in classpath");
		}

		if (!StringUtils.isEmpty(overridenConfigFile)) {
			try {
				properties.load(new ClassPathResource(overridenConfigFile).getInputStream());
			} catch (IOException e) {
				throw new RuntimeException(overridenConfigFile + "is not in classpath");
			}
		}
		return properties;
	}

	public static Object getAndVerify(Map<String, Object> map, String key, Class<?> clasz) {
		Object o = map.get(key);
		if (o == null) {
			return o;
		}
		Assert.isInstanceOf(clasz, o, String.format("key %s is expected to be %s but is of type %s with value %s",
				new Object[] { key, clasz, o.getClass().getSimpleName(), o }));
		return o;
	}
}
