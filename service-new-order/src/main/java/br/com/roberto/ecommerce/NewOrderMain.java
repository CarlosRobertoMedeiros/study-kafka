package br.com.roberto.ecommerce;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

	public static void main(String[] args) throws InterruptedException, ExecutionException {

		try(var orderDispatcher = new KafkaDispatcher<Order>()){
			try(var emailDispatcher = new KafkaDispatcher<String>()){
				
				var email = Math.random() + "@email.com";
				for (int i=0; i<=10;i++) {
				
					var orderId = UUID.randomUUID().toString();
					var amount = new BigDecimal(Math.random() * 5000 + 1);
					var order =  new Order(orderId, amount, email);
									
					orderDispatcher.send("ECOMMERCE_NEW_ORDER", email , order);
					
					var emailCode="Obrigado por sua ordem ! Iremos processar !";
					emailDispatcher.send("ECOMMERCE_SEND_EMAIL", email , emailCode);
				}
			}
		}

	}
}

// Continuar daqui E:\Estudos\Video Aulas\Desenvolvimento\Alura\Kafka\02 - Fast delegate, evolução e cluster de broker\03 - Servidor HTTP