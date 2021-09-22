package br.com.roberto.ecommerce;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.roberto.ecommerce.consumer.ConsumerService;
import br.com.roberto.ecommerce.consumer.ServiceRunner;
import br.com.roberto.ecommerce.dispatcher.KafkaDispatcher;

public class FraudeDetectorService implements ConsumerService<Order> {

	private final LocalDatabase database;

	FraudeDetectorService() throws SQLException {
		this.database = new LocalDatabase("frauds_database");
		this.database
				.createIfNotExists("create table Orders (" + "uuid varchar(200) primary key," + "is_fraud boolean");
	}

	public static void main(String[] args) {
		new ServiceRunner<>(FraudeDetectorService::new).start(1);
	}

	private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

	public void parse(ConsumerRecord<String, Message<Order>> record)
			throws InterruptedException, ExecutionException, SQLException {

		System.out.println("---------------------------------------------");
		System.out.println("Processando a nova Ordem, checando fraudes");
		System.out.println(record.key());
		System.out.println(record.value());
		System.out.println(record.partition());
		System.out.println(record.offset());

		var message = record.value();
		var order = message.getPayLoad();
		if (wasProcessed(order)) {
			System.out.println("Ordem " + order.getOrderId() + " já processada !");
			return;
		}

		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// ignora
			e.printStackTrace();
		}

		if (isFraude(order)) {
			database.update("insert into Orders (uuid,is_fraud) values (?, true)", order.getOrderId());
			// Simulate The fraud happens when the amount is >=4500
			System.out.println("Order is a Fraud " + order);
			orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(),
					message.getId().continueWith(FraudeDetectorService.class.getSimpleName()), order);
		} else {
			database.update("insert into Orders (uuid,is_fraud) values (?, false)", order.getOrderId());
			System.out.println("Approved: " + order);
			orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(),
					message.getId().continueWith(FraudeDetectorService.class.getSimpleName()), order);
		}

	}

	private boolean wasProcessed(Order order) throws SQLException {
		var results = database.query("select uuid from Orders where uuid = ? limit 1", order.getOrderId());
		return results.next();
	}

	private boolean isFraude(Order order) {
		return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
	}

	@Override
	public String getTopic() {
		return "ECOMMERCE_NEW_ORDER";
	}

	@Override
	public String getConsumerGroup() {
		return FraudeDetectorService.class.getSimpleName();
	}

}
