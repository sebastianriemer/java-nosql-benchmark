package com.github.sebastianriemer.nosqlbenchmark;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

public class TestAppendKeyValue implements TestCase, Runnable {

	private DBClient dbClient;
	private List<String> testValues;
	private String messagePrefix = null;
	private static final Logger logger = Logger
			.getLogger(TestAppendKeyValue.class);
	
	private String name = new String(this.getClass().getSimpleName());
	
	/* Getter for name */
	public String getName() {
		return name;
	}
	
	/* constructor */
	public TestAppendKeyValue(DBClient dbClient) {
		this.dbClient = dbClient;
		this.messagePrefix = new String();
	}
	
	public TestAppendKeyValue(DBClient dbClient, String messagePrefix) {
		this.dbClient = dbClient;
		this.messagePrefix = messagePrefix;
	}

	/* initialize ArrayList with test values depending on loadSize */
	@Override
	public void prepare(int loadSize) {
		testValues = new ArrayList<String>();
		for (int i = 0; i < loadSize; i++) {
			testValues.add(messagePrefix + "-MSG" + String.format("%010d", i));			
		}
	}

	/* execute test */
	@Override
	public void execute() {

		if (dbClient != null) {
			dbClient.openConnection();
			for (int i = 0; i < testValues.size(); i++) {
				dbClient.append(Integer.toString(i), testValues.get(i));
			}
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
