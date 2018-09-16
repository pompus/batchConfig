package com.kafka.configuration;

import org.springframework.stereotype.Component;

@Component
public interface JsonSerializer<T> {
	T toJson(Object domainObject);

	<D> D fromJson(T json, Class<D> clasz);
}
