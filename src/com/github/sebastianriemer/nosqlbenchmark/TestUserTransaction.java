package com.github.sebastianriemer.nosqlbenchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;

public class TestUserTransaction implements TestCase, Runnable {

	private DBClient dbClient;
	private List<String> testValues;
	private int messagePrefix;
	private static final Logger logger = Logger
			.getLogger(TestUserTransaction.class);
	
	private String name = new String(this.getClass().getSimpleName());
	
	/* Getter for name */
	public String getName() {
		return name;
	}
	
	/* constructor */
	public TestUserTransaction(DBClient dbClient) {
		this.dbClient = dbClient;
		this.messagePrefix = 0;
	}
	
	public TestUserTransaction(DBClient dbClient, int messagePrefix) {
		this.dbClient = dbClient;
		this.messagePrefix = messagePrefix;
	}

	/* initialize ArrayList with test values depending on loadSize */
	@Override
	public void prepare(int loadSize) {
		testValues = new ArrayList<String>();
		for (int i = 0; i < loadSize; i++) {			
			testValues.add("MSG" + String.format("%010d", i));			
		}		
	}
	
	public void insert() {
		for (int i = 0; i < testValues.size(); i++) {
			dbClient.append(Integer.toString(messagePrefix+i) , Integer.toString(messagePrefix)+"-"+testValues.get(i));
		}
	}
	
	public void update() {
		for (int i = 0; i < testValues.size(); i++) {
			dbClient.append(Integer.toString(messagePrefix+i), "UPD" + Integer.toString(messagePrefix)+"-"+testValues.get(i));
		}
	}
	
	public void delete(int key) {		
			dbClient.delete(Integer.toString(messagePrefix+key));		
	}
	
	public void selectAll() {
		@SuppressWarnings("unused")
		String returnValue;
		for (int i = 0; i < testValues.size(); i++) {
			returnValue = dbClient.get(Integer.toString(messagePrefix+i));
		}
	}
	
	public String select(int key) {		
			return dbClient.get(Integer.toString(messagePrefix+key));		
	}

	/* execute test */
	@Override
	public void execute() {
		@SuppressWarnings("unused")
		String returnValue;
		Random rand =  new Random();
		
		if (dbClient != null) {
			dbClient.openConnection();
			// insert all testValues
			insert();
			//select all values but do nothing with them
			selectAll();
			// update all values
			update();						
			// select a random value 
			int randomNum = rand.nextInt(testValues.size());						
			returnValue = select(randomNum);
			// delete the random value
			delete(randomNum);
			
			dbClient.closeConnection();
		} else {
			logger.error("dbClient not set!");
			System.exit(1);
		}
	}

	/* cleanup database */
	@Override
	public void cleanup() {
		dbClient.openConnection();
		dbClient.cleanup();
		dbClient.closeConnection();
	}

	@Override
	public void run() {		
		execute();
		
	}

}
