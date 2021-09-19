package br.com.roberto.ecommerce;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.roberto.ecommerce.consumer.ConsumerService;
import br.com.roberto.ecommerce.consumer.ServiceRunner;

public class EmailService implements ConsumerService<String> {

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		new ServiceRunner(EmailService::new).start(5);
	}
	
	public String getConsumerGroup() {
		return EmailService.class.getSimpleName();
	}
	
	public String getTopic() {
		return "ECOMMERCE_SEND_EMAIL";
	}

	public void parse(ConsumerRecord<String, Message<String>> record) {

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
