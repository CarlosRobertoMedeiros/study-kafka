package br.com.roberto.ecommerce;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.eclipse.jetty.servlet.Source;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class GenerateAllReportsServlet extends HttpServlet {
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private final KafkaDispatcher<String> batchDispatcher = new KafkaDispatcher<>();


	@Override
	public void destroy() {
		batchDispatcher.close();
		super.destroy();
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

		try {
			
			batchDispatcher.send("SEND_MESSAGE_TO_ALL_USERS","USER_GENERATE_READING_REPORT","USER_GENERATE_READING_REPORT");
			
			

			System.out.println("Enviando relatório gerado para todos usuários");
			resp.setStatus(HttpServletResponse.SC_OK);
			resp.getWriter().println("Requisição de Relatórios gerados");

		} catch (InterruptedException | ExecutionException e) {
			throw new ServletException(e);
		}

	}

}
