package br.com.roberto.ecommerce;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService {

	public static void main(String[] args) {
		var emailService = new EmailService();
		try(var service = new KafkaService<>(EmailService.class.getSimpleName(), 
									   "ECOMMERCE_SEND_EMAIL", 
									   emailService::parse,
									   String.class,
									   Map.of())){
			service.run();
		}
	}

	private void parse(ConsumerRecord<String, Message<String>> record) {

		System.out.println("---------------------------------------------");
		System.out.println("Enviando Email !");
		System.out.println(record.key());
		System.out.println(record.value());
		System.out.println(record.partition());
		System.out.println(record.offset());

		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// ignora
			e.printStackTrace();
		}

		System.out.println("o email foi enviado !");
	}

}
