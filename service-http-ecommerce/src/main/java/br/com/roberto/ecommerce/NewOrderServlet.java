package br.com.roberto.ecommerce;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import br.com.roberto.ecommerce.dispatcher.KafkaDispatcher;
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

	@Override
	public void destroy() {
		orderDispatcher.close();
		super.destroy();
		
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

		try {

			// starting point
			//need be thin because we need reproduce both parts. 
			var email = req.getParameter("email");
			var amount = new BigDecimal(req.getParameter("amount"));
			var orderId = req.getParameter("uuid");// //UUID.randomUUID().toString();
			var order = new Order(orderId, amount, email);
			
			try(var database = new OrdersDatabase()){
			
				if (database.saveNew(order)) {
	
					orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, new CorrelationId(NewOrderServlet.class.getSimpleName()), order);
					System.out.println("New Order sent successfully.");
					resp.setStatus(HttpServletResponse.SC_OK);
					resp.getWriter().println("New Order sent ");
				}else {
					System.out.println("Old Order received.");
					resp.setStatus(HttpServletResponse.SC_OK);
					resp.getWriter().println("Old Order received. ");
				}
			}

		} catch (InterruptedException | ExecutionException | SQLException e) {
			throw new ServletException(e);
		}

	}

}
