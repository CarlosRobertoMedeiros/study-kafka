package br.com.roberto.ecommerce;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.roberto.ecommerce.consumer.KafkaService;
import br.com.roberto.ecommerce.dispatcher.KafkaDispatcher;

public class FraudeDetectorService {

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		var fraudeDetectorService = new FraudeDetectorService();
		try(var service = new KafkaService<Order>(FraudeDetectorService.class.getSimpleName(),
									 "ECOMMERCE_NEW_ORDER", 
									 fraudeDetectorService::parse, 
									 Map.of())){
			service.run();
		}
	}
	
	private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

	private void parse(ConsumerRecord<String, Message<Order>> record) throws InterruptedException, ExecutionException {

		System.out.println("---------------------------------------------");
		System.out.println("Processando a nova Ordem, checando fraudes");
		System.out.println(record.key());
		System.out.println(record.value());
		System.out.println(record.partition());
		System.out.println(record.offset());

		var message = record.value();
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// ignora
			e.printStackTrace();
		}
		
		var order = message.getPayLoad();
		
		if (isFraude(order)) {
			//Simulate The fraud happens when the amount is >=4500
			System.out.println("Order is a Fraud "+order);
			orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(),
					message.getId().continueWith(FraudeDetectorService.class.getSimpleName()), order);
		}else {
			System.out.println("Approved: "+order);
			orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(),
					message.getId().continueWith(FraudeDetectorService.class.getSimpleName()), order);
		}
		
	}

	private boolean isFraude(Order order) {
		return order.getAmount().compareTo(new BigDecimal("4500")) >=0;
	}

}
