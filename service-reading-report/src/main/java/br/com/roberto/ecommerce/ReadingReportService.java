package br.com.roberto.ecommerce;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.roberto.ecommerce.consumer.ConsumerService;
import br.com.roberto.ecommerce.consumer.ServiceRunner;

public class ReadingReportService implements ConsumerService<User> {

	private static final Path SOURCE = new File("src/main/resources/reports.txt").toPath();

	public static void main(String[] args){
		new ServiceRunner(ReadingReportService::new).start(4);
	}

	@Override
	public void parse(ConsumerRecord<String, Message<User>> record) throws IOException {

		System.out.println("---------------------------------------------");
		System.out.println("Processando relatório para " + record.value());

		var message = record.value();
		var user = message.getPayLoad();
		var target = new File(user.getReportPath());
		IO.copyTo(SOURCE, target);
		IO.append(target, "Created for " + user.getUuid());

		System.out.println("File created: " + target.getAbsolutePath());

	}

	@Override
	public String getTopic() {
		return "ECOMMERCE_USER_GENERATE_READING_REPORT";
	}

	@Override
	public String getConsumerGroup() {
		return ReadingReportService.class.getSimpleName();
	}

}
