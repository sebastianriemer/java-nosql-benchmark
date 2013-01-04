package com.github.sebastianriemer.nosqlbenchmark;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
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

	private static List<String> testValues;
	private static List<JSON> testJSONs;

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
			for (int i = 0; i < testValues.size(); i++) {
				// System.out.println("Appending key="+testData.get(i) +
				// " value=" +Integer.toString(i));
				dbClient.append(testValues.get(i), Integer.toString(i));
			}
			dbClient.closeConnection();
		} else {
			logger.severe("dbClient not set!");
			System.exit(1);
		}
	}

	/*private static void execute_testCase3() {
		if (dbClient != null) {
			dbClient.openConnection();

//			JSON json = ConvertXMLToJSON.convertXmlToJson("test2.xml");
			for (JSON json: testJSONs) {
				dbClient.append("50002", json);
			}

			dbClient.closeConnection();
		} else {
			logger.severe("dbClient not set!");
			System.exit(1);
		}
	}*/

	public static void execute(String testCaseName) {

		if (testCaseName.equals(NosqlBenchmark.TC_1)) {
			execute_testCase1();
		} else if (testCaseName.equals(NosqlBenchmark.TC_2)) {
			execute_testCase2();
		/*} else if (testCaseName.equals(NosqlBenchmark.TC_3)) {
			execute_testCase3();*/
		} else {
			logger.severe(String.format("Unknown Test Case %s", testCaseName));
		}

	}

	public static void prepare(String testCaseName, int loadSize) {

		if (testCaseName.equals(NosqlBenchmark.TC_1)) {
			prepare_testCase1();
		} else if (testCaseName.equals(NosqlBenchmark.TC_2)) {
			prepare_testCase2(loadSize);
		/*} else if (testCaseName.equals(NosqlBenchmark.TC_3)) {
			prepare_testCase3(loadSize);*/
		} else {
			logger.severe(String.format("Unknown Test Case %s", testCaseName));
		}

	}

	public static void prepare_testCase3(int loadSize) {
		initialize_xmlfiles(loadSize);

	}

	public static void prepare_testCase2(int loadSize) {
		generate_testarray(loadSize);
	}

	public static void prepare_testCase1() {
		// TODO Auto-generated method stub

	}

	public static void initialize_xmlfiles(int size) {
		// prepare TestData
		testJSONs = new ArrayList<JSON>();
		int i = 0;			
		
		 try {
			System.out.println(new File(".").getCanonicalPath());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
		 URL url = TestCases.class.getResource("resources/xmlfiles");
		 try {
			File dir = new File(url.toURI());
			for (File child : dir.listFiles()) {
			    // Do something with child
				  //System.out.println("Trying to get JSON from XML with name \"" +child.getName() +"\"");
				  testJSONs.add(ConvertXMLToJSON.convertXmlToJson(child.getName()));
				  i++;
				  if (i>size)
					  break;
			  }
	
			
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}		 				  
		
	}
	
	public static void generate_testarray(int size) {
		// prepare TestData
		testValues = new ArrayList<String>();
		for (int i = 0; i < size; i++) {
			testValues.add("MSG" + String.format("%010d", i));
		}
	}
	
	public static void cleanup(String testCaseName) {

		if (testCaseName.equals(NosqlBenchmark.TC_1)) {
			cleanup_testCase1();
		} else if (testCaseName.equals(NosqlBenchmark.TC_2)) {
			cleanup_testCase2();
		/*} else if (testCaseName.equals(NosqlBenchmark.TC_3)) {
			cleanup_testCase3();*/
		} else {
			logger.severe(String.format("Unknown Test Case %s", testCaseName));
		}

	}

	public static void cleanup_testCase3() {
		dbClient.openConnection();	
		dbClient.cleanup();
		dbClient.closeConnection();

	}

	public static void cleanup_testCase2() {
		dbClient.openConnection();
		dbClient.cleanup();
		dbClient.closeConnection();
	}

	public static void cleanup_testCase1() {
		dbClient.openConnection();
		dbClient.cleanup();
		dbClient.closeConnection();

	}	
	
}
