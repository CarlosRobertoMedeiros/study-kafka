package br.com.roberto.ecommerce;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class NewOrderMain {

	public static void main(String[] args) throws InterruptedException, ExecutionException {

		var producer = new KafkaProducer<String, String>(properties());
		var value = "132123,67523,789456";
		var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", value, value);
		producer.send(record, (data, ex) ->{
			if(ex !=null) {
				throw new RuntimeException(ex.getCause() +" - " +  ex.getMessage());
			}
			System.out.println("Sucesso ! "+ data.topic()+":::: partição "+ 
							   data.partition()+":::: offset "+
							   data.offset()+":::: timestamp "+
							   data.timestamp());
			
		}).get(); //Future

	}

	private static Properties properties() {
		var properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return properties;
	}

}
