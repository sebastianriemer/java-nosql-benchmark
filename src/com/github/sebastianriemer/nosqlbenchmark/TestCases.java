package com.github.sebastianriemer.nosqlbenchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;

import net.sf.json.JSON;

public final class TestCases {
	public static DBClient dbClient;
	private static final Logger logger = Logger.getLogger(TestCases.class
			.getName());

	private static List<String> testData;

	// Static constructor
	static {
		SimpleFormatter fmt = new SimpleFormatter();
		StreamHandler sh = new StreamHandler(System.out, fmt);

		logger.addHandler(sh);
		logger.setUseParentHandlers(false);
	}

	private static void execute_testCase1() {
		if (dbClient != null) {
			dbClient.openConnection();
			dbClient.append("12345", "AAA");
			String value = dbClient.get("12345");
			logger.info("Value of key = 12345 is \"" + value + "\"");
			dbClient.closeConnection();
		} else {
			logger.severe("dbClient not set!");
			System.exit(1);
		}

	}

	private static void execute_testCase2() {
		if (dbClient != null) {
			dbClient.openConnection();
			for (int i = 0; i < testData.size(); i++) {
				// System.out.println("Appending key="+testData.get(i) +
				// " value=" +Integer.toString(i));
				dbClient.append(testData.get(i), Integer.toString(i));
			}
			dbClient.closeConnection();
		} else {
			logger.severe("dbClient not set!");
			System.exit(1);
		}
	}

	private static void execute_testCase3() {
		if (dbClient != null) {
			dbClient.openConnection();

			JSON json = ConvertXMLToJSON.convertXmlToJson("test2.xml");

			dbClient.append("50002", json);

			dbClient.closeConnection();
		} else {
			logger.severe("dbClient not set!");
			System.exit(1);
		}
	}

	public static void execute(String testCaseName) {

		if (testCaseName.equals(NosqlBenchmark.TC_1)) {
			execute_testCase1();
		} else if (testCaseName.equals(NosqlBenchmark.TC_2)) {
			execute_testCase2();
		} else if (testCaseName.equals(NosqlBenchmark.TC_3)) {
			execute_testCase3();
		} else {
			logger.severe(String.format("Unknown Test Case %s", testCaseName));
		}

	}

	public static void prepare(String testCaseName) {

		if (testCaseName.equals(NosqlBenchmark.TC_1)) {
			prepare_testCase1();
		} else if (testCaseName.equals(NosqlBenchmark.TC_2)) {
			prepare_testCase2();
		} else if (testCaseName.equals(NosqlBenchmark.TC_3)) {
			prepare_testCase3();
		} else {
			logger.severe(String.format("Unknown Test Case %s", testCaseName));
		}

	}

	public static void prepare_testCase3() {
		// TODO Auto-generated method stub

	}

	public static void prepare_testCase2() {
		load_testdata(1000);
	}

	public static void prepare_testCase1() {
		// TODO Auto-generated method stub

	}

	public static void load_testdata(int size) {
		// prepare TestData
		testData = new ArrayList<String>();
		for (int i = 0; i < size; i++) {
			testData.add("MSG" + String.format("%010d", i));
		}

	}
}
