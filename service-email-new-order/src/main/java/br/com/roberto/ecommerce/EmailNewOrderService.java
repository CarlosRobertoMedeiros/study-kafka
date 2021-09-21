package br.com.roberto.ecommerce;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.roberto.ecommerce.consumer.ConsumerService;
import br.com.roberto.ecommerce.consumer.ServiceRunner;
import br.com.roberto.ecommerce.dispatcher.KafkaDispatcher;

public class EmailNewOrderService implements ConsumerService<Order> {

	public static void main(String[] args) {
		new ServiceRunner(EmailNewOrderService::new).start(1);
	}
	
	private final KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<>();

	public void parse(ConsumerRecord<String, Message<Order>> record) throws InterruptedException, ExecutionException {

		System.out.println("---------------------------------------------");
		System.out.println("Processando a nova Ordem, preparando email");
		var message = record.value();
		System.out.println(message);
		
		
		var order = message.getPayLoad();
		var emailCode="Obrigado por sua ordem ! Iremos processar !";
		var id = message.getId().continueWith(EmailNewOrderService.class.getSimpleName());
		emailDispatcher.send("ECOMMERCE_SEND_EMAIL", order.getEmail() , id , emailCode);
	}

	@Override
	public String getTopic() {
		return "ECOMMERCE_NEW_ORDER";
	}

	@Override
	public String getConsumerGroup() {
		return EmailNewOrderService.class.getSimpleName();
	}


}
