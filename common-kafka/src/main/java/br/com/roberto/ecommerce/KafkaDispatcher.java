package br.com.roberto.ecommerce;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

class KafkaDispatcher<T> implements Closeable{
	
	private KafkaProducer<String, T> producer;


	KafkaDispatcher(){
		this.producer = new KafkaProducer<>(properties());
	}

	
	void send(String topic, String key, T value) throws InterruptedException, ExecutionException {
		
		var record = new ProducerRecord<>(topic, key, value);
		
		Callback callback = (data, ex) ->{
			if(ex !=null) {
				throw new RuntimeException(ex.getCause() +" - " +  ex.getMessage());
			}
			System.out.println("Sucesso ! "+ data.topic()+":::: partição "+ 
							   data.partition()+":::: offset "+
							   data.offset()+":::: timestamp "+
							   data.timestamp());
			
		};
		producer.send(record, callback).get(); //Future
	}
	
	private static Properties properties() {
		var properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
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
