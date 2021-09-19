package br.com.roberto.ecommerce;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.roberto.ecommerce.consumer.KafkaService;
import br.com.roberto.ecommerce.dispatcher.KafkaDispatcher;

public class EmailNewOrderService {

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		var emailService = new EmailNewOrderService();
		try(var service = new KafkaService<Order>(EmailNewOrderService.class.getSimpleName(),
									 "ECOMMERCE_NEW_ORDER", 
									 emailService::parse, 
									 Map.of())){
			service.run();
		}
	}
	
	private final KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<>();

	private void parse(ConsumerRecord<String, Message<Order>> record) throws InterruptedException, ExecutionException {

		System.out.println("---------------------------------------------");
		System.out.println("Processando a nova Ordem, preparando email");
		var message = record.value();
		System.out.println(message);
		
		
		var order = message.getPayLoad();
		var emailCode="Obrigado por sua ordem ! Iremos processar !";
		var id = message.getId().continueWith(EmailNewOrderService.class.getSimpleName());
		emailDispatcher.send("ECOMMERCE_SEND_EMAIL", order.getEmail() , id , emailCode);
	}


}
