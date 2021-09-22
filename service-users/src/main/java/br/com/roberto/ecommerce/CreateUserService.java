package br.com.roberto.ecommerce;

import java.sql.SQLException;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.roberto.ecommerce.consumer.ConsumerService;
import br.com.roberto.ecommerce.consumer.ServiceRunner;

public class CreateUserService implements ConsumerService<Order> {

	private final LocalDatabase database;

	CreateUserService() throws SQLException {
		this.database = new LocalDatabase("users_database");
		this.database
				.createIfNotExists("create table Users (" + "uuid varchar(200) primary key," + "email varchar(200)");
	}

	public static void main(String[] args) {
		new ServiceRunner<>(CreateUserService::new).start(1);

	}

	@Override
	public void parse(ConsumerRecord<String, Message<Order>> record) throws SQLException {

		System.out.println("---------------------------------------------");
		System.out.println("Processando a nova Ordem, checando para novos usu�rios");
		System.out.println(record.value());

		var order = record.value().getPayLoad();

		if (isNewUser(order.getEmail())) {
			insertNewUser(order.getEmail());
		}

	}

	private void insertNewUser(String email) throws SQLException {
		var uuid = UUID.randomUUID().toString();
		database.update("insert into Users (uuid, email ) " + " values (? , ?) ", uuid, email);
		System.out.println("Usu�rio " + uuid + " e " + email + " adicionado");
	}

	private boolean isNewUser(String email) throws SQLException {
		var results = database.query("select uuid from Users \" + \" where email = ? limit 1 ", email);
		return !results.next();
	}

	@Override
	public String getTopic() {
		return "ECOMMERCE_NEW_ORDER";
	}

	@Override
	public String getConsumerGroup() {
		return CreateUserService.class.getSimpleName();
	}

}
