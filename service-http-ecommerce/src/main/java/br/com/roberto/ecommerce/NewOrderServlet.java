package br.com.roberto.ecommerce;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class NewOrderServlet extends HttpServlet {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();
	private final KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<>();

	@Override
	public void destroy() {
		orderDispatcher.close();
		emailDispatcher.close();
		super.destroy();
		
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

		try {

			// starting point
			//need be thin because we need reproduce both parts. 
			var email = req.getParameter("email");
			var amount = new BigDecimal(req.getParameter("amount"));

			var orderId = UUID.randomUUID().toString();
			var order = new Order(orderId, amount, email);

			orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, new CorrelationId(NewOrderServlet.class.getSimpleName()), order);

			var emailCode = "Obrigado por sua ordem ! Iremos processar !";
			emailDispatcher.send("ECOMMERCE_SEND_EMAIL", email, new CorrelationId(NewOrderServlet.class.getSimpleName()), emailCode);

			System.out.println("New Order sent successfully.");

			resp.getWriter().println("New Order sent ");
			resp.setStatus(HttpServletResponse.SC_OK);

		} catch (InterruptedException | ExecutionException e) {
			throw new ServletException(e);
		}

	}

}
