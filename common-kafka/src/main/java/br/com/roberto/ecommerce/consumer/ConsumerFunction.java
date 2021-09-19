package br.com.roberto.ecommerce.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.roberto.ecommerce.Message;

public interface ConsumerFunction<T> {
	void consume(ConsumerRecord<String, Message<T>> record) throws Exception;
}
