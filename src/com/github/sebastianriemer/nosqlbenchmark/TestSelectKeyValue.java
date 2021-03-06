package com.github.sebastianriemer.nosqlbenchmark;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;

public class TestSelectKeyValue implements TestCase {

	private DBClient dbClient;
	private static List<String> testValues;
	private static final Logger logger = Logger
			.getLogger(TestSelectKeyValue.class);

	private String name = new String(this.getClass().getSimpleName());
	
	/* Getter for name */
	public String getName() {
		return name;
	}

	/* constructor */
	public TestSelectKeyValue(DBClient dbClient) {
		this.dbClient = dbClient;
	}

	/* initialize ArrayList with test values depending on loadSize */
	@Override
	public void prepare(int loadSize) {
		testValues = new ArrayList<String>();
		for (int i = 0; i < loadSize; i++) {
			testValues.add("MSG" + String.format("%010d", i));
		}
		if (dbClient != null) {
			dbClient.openConnection();
			for (int i = 0; i < testValues.size(); i++) {
				dbClient.append(Integer.toString(i),testValues.get(i));
			}
			dbClient.closeConnection();
		} else {
			logger.error("dbClient not set!");
			System.exit(1);
		}
	}

	/* execute test */
	@Override
	public void execute() {
		@SuppressWarnings("unused")
		String returnValue = null;
		if (dbClient != null) {
			dbClient.openConnection();
			for (int i = 0; i < testValues.size(); i++) {
				returnValue = dbClient.get(Integer.toString(i));
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
		// TODO Auto-generated method stub
		throw new NotImplementedException();
	}
}
