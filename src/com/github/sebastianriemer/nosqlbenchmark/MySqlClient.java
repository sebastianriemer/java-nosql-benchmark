package com.github.sebastianriemer.nosqlbenchmark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;

import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;

import net.sf.json.JSON;

public class MySqlClient implements DBClient {
	private static final Logger logger = Logger.getLogger(MySqlClient.class
			.getName());
	private Connection connect = null;

	/* append JSON object with given key */
	public void append(String key, JSON jsonObj) {
		try {
			PreparedStatement preparedStatement;
			preparedStatement = connect
					.prepareStatement("insert into test_smalltext (keyid, value) values (?,?) on duplicate key update value=?");
			preparedStatement.setString(1, key);
			preparedStatement.setString(2, jsonObj.toString());
			preparedStatement.setString(3, jsonObj.toString());
			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			logger.error("SQLException while trying to append value! "
					+ e.getMessage());
		}
	}

	/* append string with the given key */
	public void append(String key, String value) {
		try {
			PreparedStatement preparedStatement;
			preparedStatement = connect
					.prepareStatement("insert into test_dictionary (keyid, value) values (?,?) on duplicate key update value=?");
			preparedStatement.setString(1, key);
			preparedStatement.setString(2, value);
			preparedStatement.setString(3, value);
			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			logger.error("SQLException while trying to append value! "
					+ e.getMessage());
		}
	}

	/* return the value of the given key 
	 * as for the mysql schema definition, there are two tables
	 * where values may be stored, so both of them must be queried 
	 * */
	public String get(String key) {
		int exCount = 0;
		try {
			PreparedStatement preparedStatement;
			preparedStatement = connect
					.prepareStatement("select value from test_dictionary WHERE keyid=?");
			preparedStatement.setString(1, key);
			ResultSet resultSet = preparedStatement.executeQuery();
								
			if (resultSet.next()) {
				return resultSet.getString("value");
			}
			else {
				//logger.error("Error while getting value! No value returned!");
				exCount = exCount +1;
			}			
		} catch (SQLException e) {
			logger.error("SQLException occured! "+ e.getMessage());
		}
		try {
			PreparedStatement preparedStatement;
			preparedStatement = connect
					.prepareStatement("select value from test_smalltext WHERE keyid=?");
			preparedStatement.setString(1, key);
			ResultSet resultSet = preparedStatement.executeQuery();
								
			if (resultSet.next()) {
				return resultSet.getString("value");
			}
			else {
				//logger.error("Error while getting value! No value returned!");
				exCount = exCount +1;
			}			
		} catch (SQLException e) {
			logger.error("SQLException occured! "+ e.getMessage());			
		}
		if (exCount == 2)
			logger.error("SQLException raised, no data found while trying to get value using keyId="+key+" !");
		return null;
	}

	/* delete all data from db */
	public void cleanup() {
		try {
			PreparedStatement preparedStatement;
			preparedStatement = connect
					.prepareStatement("truncate table test_dictionary");
			preparedStatement.execute();
			preparedStatement = connect
					.prepareStatement("truncate table test_smalltext");
			preparedStatement.execute();
		} catch (SQLException e) {
			logger.error("SQLException while trying to truncate table! "
					+ e.getMessage());
		}
	}

	/* open a connection */
	public void openConnection() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			logger.info(String.format("Opening Connection to localhost ..."));
			connect = DriverManager
					.getConnection("jdbc:mysql://localhost/test?user=testUser&password=test");
		} catch (ClassNotFoundException e) {
			logger.error("ClassNotFoundException while trying to open connection! "
					+ e.getMessage());
		} catch (SQLException e) {
			logger.error("SQLException while trying to open connection! "
					+ e.getMessage());
		}

	}

	/* close the connection */
	public void closeConnection() {
		try {
			logger.info(String.format("Closing Connection ..."));
			if (connect != null) {
				connect.close();
			}
		} catch (SQLException e) {
			logger.error("SQLException while trying to close connection! "
					+ e.getMessage());
		}
	}

	/* initial testing method */
	@Override
	public void firstTest() {
		// TODO Auto-generated method stub
		throw new NotImplementedException();
	}

	/* remove key */
	@Override
	public void delete(String key) {
		int exCount = 0;
		try {
			PreparedStatement preparedStatement;
			preparedStatement = connect
					.prepareStatement("delete from test_dictionary where keyid = ?");
			preparedStatement.setString(1, key);			
			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			exCount = exCount +1;
		}
		try {
			PreparedStatement preparedStatement;
			preparedStatement = connect
					.prepareStatement("delete from test_smalltext where keyid = ?");
			preparedStatement.setString(1, key);			
			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			exCount = exCount +1;			
		}
		if (exCount == 2) {
			logger.error("SQLException while trying to delete value!");
		}
	}
	

}
