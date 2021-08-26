package br.com.roberto.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudeDetectorService {

	public static void main(String[] args) {
		var fraudeDetectorService = new FraudeDetectorService();
		try(var service = new KafkaService(FraudeDetectorService.class.getSimpleName(),
									 "ECOMMERCE_NEW_ORDER", 
									 fraudeDetectorService::parse)){
			service.run();
		}
	}

	private void parse(ConsumerRecord<String, String> record) {

		System.out.println("---------------------------------------------");
		System.out.println("Processando a nova Ordem, checando fraudes");
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

		System.out.println("a Ordem foi processada !");

	}

}
