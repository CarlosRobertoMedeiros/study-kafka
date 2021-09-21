package br.com.roberto.ecommerce.consumer;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.roberto.ecommerce.Message;

public interface ConsumerService<T> {
	void parse(ConsumerRecord<String, Message<T>> record) throws IOException, InterruptedException, ExecutionException;
	String getTopic();
	String getConsumerGroup();
}
