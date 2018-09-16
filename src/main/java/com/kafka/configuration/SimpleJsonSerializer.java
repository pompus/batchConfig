package com.kafka.configuration;

import java.io.IOException;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class SimpleJsonSerializer implements JsonSerializer<String> {
	final ObjectMapper mapper = new ObjectMapper();

	@Override
	public java.lang.String toJson(Object domainObject) {
		try {
			return mapper.writeValueAsString(domainObject);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e.getMessage());
		}
	}

	@Override
	public <D> D fromJson(String json, Class<D> clasz) {
		try {
		return mapper.readValue(json, clasz);
		}
		catch (IOException e) {
			throw new RuntimeException(e.getMessage());
		}
	}
}
