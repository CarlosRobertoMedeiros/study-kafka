package br.com.roberto.ecommerce.dispatcher;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import br.com.roberto.ecommerce.CorrelationId;
import br.com.roberto.ecommerce.Message;

public class KafkaDispatcher<T> implements Closeable{
	
	private KafkaProducer<String, Message<T>> producer;

	public KafkaDispatcher(){
		this.producer = new KafkaProducer<>(properties());
	}
	
	public void send(String topic, String key, CorrelationId id,  T payload) throws InterruptedException, ExecutionException {
		var future = sendAsync(topic, key, id, payload);
		future.get();
	}


	public Future<RecordMetadata> sendAsync(String topic, String key, CorrelationId id, T payload) {
		var value = new Message<T>(id.continueWith("_" + topic), payload);
		var record = new ProducerRecord<>(topic, key, value);
		
		Callback callback = (data, ex) ->{
			if(ex !=null) {
				throw new RuntimeException(ex.getCause() +" - " +  ex.getMessage());
			}
			System.out.println("Sucesso enviando ! "+ data.topic()+":::: partição "+ 
							   data.partition()+":::: offset "+
							   data.offset()+":::: timestamp "+
							   data.timestamp());
			
		};
		return producer.send(record, callback);
	}
	
	private static Properties properties() {
		var properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); //Garante que o lider e todas as réplicas receberam a mensagem "Reliability"
		return properties;
	}


	/**
	 * Nesse Caso não tem exceção
	 */
	@Override
	public void close() {
		producer.close();
		
	}
}
