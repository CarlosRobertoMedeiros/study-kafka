package br.com.roberto.ecommerce.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.roberto.ecommerce.Message;

public interface ConsumerService<T> {
	
	//Uma outra abordagem utilizada serua usar o ConsumerException
	void parse(ConsumerRecord<String, Message<T>> record) throws Exception;
	String getTopic();
	String getConsumerGroup();
}
