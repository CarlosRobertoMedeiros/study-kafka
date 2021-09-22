package br.com.roberto.ecommerce;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class LocalDatabase {

	private final Connection connection;

	public LocalDatabase(String databaseName) throws SQLException {
		String url = "jdbc:sqlite:target/" + databaseName + ".db";
		this.connection = DriverManager.getConnection(url);
	}

	// generic way
	// beware if sql injection
	public void createIfNotExists(String sql) {
		try {
			connection.createStatement().execute(sql);
		} catch (SQLException ex) {
			// be carefull, the sql code be wrong
			ex.printStackTrace();
		}

	}

	public boolean update(String statement, String... params) throws SQLException {
		return prepare(statement, params).execute();
	}

	public ResultSet query(String query, String... params) throws SQLException {
		return prepare(query, params).executeQuery();
	}

	private PreparedStatement prepare(String statement, String... params) throws SQLException {
		var preparedStatement = connection.prepareStatement(statement);

		for (int i = 0; i < params.length; i++) {
			preparedStatement.setString(i + 1, params[i]);
		}
		return preparedStatement;
	}

	public void close() throws SQLException {
		connection.close();
		
	}
}
