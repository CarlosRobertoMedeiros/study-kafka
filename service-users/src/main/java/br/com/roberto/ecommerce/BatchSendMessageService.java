package br.com.roberto.ecommerce;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class BatchSendMessageService {

	private Connection connection;

	public BatchSendMessageService() throws SQLException {
		String url = "jdbc:sqlite:target/users_database.db";
		this.connection = DriverManager.getConnection(url);
		
		try {
		connection.createStatement().execute("create table Users (" +
						"uuid varchar(200) primary key, " +
						"email varchar(200)) " );
		}catch(SQLException ex){
			//be carefull, the sql code be wrong
			ex.printStackTrace();
		}

		
	}
	
	public static void main(String[] args) throws SQLException {
		var batchService = new BatchSendMessageService();
		try(var service = new KafkaService<>(BatchSendMessageService.class.getSimpleName(),
									 "ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS", 
									 batchService::parse, 
									 String.class,
									 Map.of())){
			service.run();
		}
	}
	

	private final KafkaDispatcher<User> userDispatcher = new KafkaDispatcher<>();
	
	private void parse(ConsumerRecord<String, Message<String>> record) throws InterruptedException, ExecutionException, SQLException {

		System.out.println("---------------------------------------------");
		System.out.println("Processando um novo Batch");
		
		var message = record.value();
		System.out.println("Topic: "+ message.getPayLoad());

		
		for (User user : getAllUsers()) {
			userDispatcher.send(message.getPayLoad(), 
					user.getUuid(),
					message.getId().continueWith(BatchSendMessageService.class.getSimpleName()), 
					user);
		}
		
	}

	private List<User> getAllUsers() throws SQLException {
		var results = connection.prepareStatement("select uuid from Users").executeQuery();
		List<User> users = new ArrayList<>();
		while (results.next()){
			users.add(new User(results.getString("uuid")));
		}
		
		return users;
		
	}
}
