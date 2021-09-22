package br.com.roberto.ecommerce.consumer;

public interface ServiceFactory<T> {
	ConsumerService<T> create() throws Exception;
}
