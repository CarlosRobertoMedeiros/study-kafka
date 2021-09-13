package br.com.roberto.ecommerce;

public class Message<T> {

	private CorrelationId id;
	private T payLoad;

	public Message(CorrelationId id, T payLoad) {
		this.id = id;
		this.payLoad = payLoad;
	}

	@Override
	public String toString() {
		return "Message [id=" + id + ", payLoad=" + payLoad + "]";
	}
	
	public CorrelationId getId() {
		return id;
	}
	
	public T getPayLoad() {
		return payLoad;
	}

}
