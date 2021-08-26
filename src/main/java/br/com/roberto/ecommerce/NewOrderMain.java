package br.com.roberto.ecommerce;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

	public static void main(String[] args) throws InterruptedException, ExecutionException {

		try(var dispatcher = new KafkaDispatcher()){
		
			for (int i=0; i<=10;i++) {
			
				var key = UUID.randomUUID().toString();
				var value = key + ",67523,789456";
				
				dispatcher.send("ECOMMERCE_NEW_ORDER", key , value);
				
				var email="Obrigado por sua ordem ! Iremos processar !";
				dispatcher.send("ECOMMERCE_SEND_EMAIL", key , email);
			}
		}
		

	}

	

}
