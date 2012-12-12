package com.github.sebastianriemer.nosqlbenchmark;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;

import net.sf.json.JSON;


public class MySqlClient implements DBClient {
	private Logger logger;
	private Connection connect = null;

	public void append(String key, JSON jsonObj) {
		try {								
			PreparedStatement preparedStatement; 
		      preparedStatement = connect
		          .prepareStatement("insert into test_smalltext (keyid, value) values (?,?) on duplicate key update value=?");
		      preparedStatement.setString(1, key);
		      preparedStatement.setString(2, jsonObj.toString());
		      preparedStatement.setString(3, jsonObj.toString());
		      preparedStatement.executeUpdate();
			} 
			catch (SQLException e) {
				logger.severe("SQLException while trying to append value! " + e.getMessage());
			}
		
		
	}
	
	public void append(String key, String value) {
		try {								
		PreparedStatement preparedStatement; 
	      preparedStatement = connect
	          .prepareStatement("insert into test_dictionary (keyid, value) values (?,?) on duplicate key update value=?");
	      preparedStatement.setString(1, key);
	      preparedStatement.setString(2, value);
	      preparedStatement.setString(3, value);
	      preparedStatement.executeUpdate();
		} 
		catch (SQLException e) {
			logger.severe("SQLException while trying to append value! " + e.getMessage());
		}
	}

	public String get(String key) {
		try {
		 Statement statement = null;		 
		 ResultSet resultSet = null;
		 
		statement = connect.createStatement();
	      // Result set get the result of the SQL query
	      resultSet = statement
	          .executeQuery("select value from test_dictionary WHERE keyId=" + key);
		
		  /*resultSet = statement.executeQuery("show grants");
		  System.out.println("Result= " + resultSet.getString(1));*/
	      if(resultSet.next()){
	    	  return resultSet.getString("value");
	      }
	      return null;
		}
		catch (SQLException e) {
			logger.severe("SQLException while trying to get value! " + e.getMessage());
		}		
		return null;
	}

	
	public void openConnection() {
		try 
		{
			Class.forName("com.mysql.jdbc.Driver");
			connect = DriverManager
			          .getConnection("jdbc:mysql://localhost/test?user=testUser&password=test");
			logger.info("Connection established ...");			
		}
		catch (ClassNotFoundException e)
		{
			logger.severe("ClassNotFoundException while trying to open connection! " + e.getMessage());
		}
		catch (SQLException e)
		{
			logger.severe("SQLException while trying to open connection! " + e.getMessage());
		}
		
	}

	public void closeConnection() {
		try {
		  if (connect != null) {
		        connect.close();
		      }
		} catch (SQLException e) {
			logger.severe("SQLException while trying to close connection! " + e.getMessage());
	    }
		logger.info("Connection closed!");
	}

	public MySqlClient() {

		this.logger = Logger.getLogger(MySqlClient.class.getName());
		logger.setLevel(Level.INFO);

	}

	public void firstTest() {
	}

}
