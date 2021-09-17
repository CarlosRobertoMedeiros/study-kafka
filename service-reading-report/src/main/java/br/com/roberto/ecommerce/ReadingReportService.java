package br.com.roberto.ecommerce;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ReadingReportService {

	private static final Path SOURCE = new File("src/main/resources/reports.txt").toPath();

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		var reportService = new ReadingReportService();
		try (var service = new KafkaService<User>(ReadingReportService.class.getSimpleName(),
				"ECOMMERCE_USER_GENERATE_READING_REPORT", reportService::parse, Map.of())) {
			service.run();
		}
	}

	private void parse(ConsumerRecord<String, Message<User>> record)
			throws IOException {

		System.out.println("---------------------------------------------");
		System.out.println("Processando relatório para " + record.value());

		var message = record.value();
		var user = message.getPayLoad();
		var target = new File(user.getReportPath());
		IO.copyTo(SOURCE, target);
		IO.append(target, "Created for " + user.getUuid());
		
		System.out.println("File created: " + target.getAbsolutePath());
	}

}
