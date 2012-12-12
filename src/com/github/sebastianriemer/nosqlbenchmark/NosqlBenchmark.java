package com.github.sebastianriemer.nosqlbenchmark;

import java.util.ArrayList;
import java.util.Date;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;


public class NosqlBenchmark {
	private static final Logger logger = Logger.getLogger(NosqlBenchmark.class
			.getName());;

	private String dbName;
	private String testCaseName;


	public final static String TC_1 = "TestAppendOne";
	public final static String TC_2 = "TestAppend1000";
	public final static String TC_3 = "TestAppendJSON";
	
	public final static String DB_REDIS = "Redis";
	public final static String DB_MONGODB = "MongoDB";
	public final static String DB_MYSQL = "MySql";

	public static ArrayList<String> testCases = new ArrayList<String>();
	public static ArrayList<String> dbs = new ArrayList<String>();

	/* Printing out usage of program */
	public static void usage() {
		String validTestCasesString;
		String validDBsString;
		System.out.println("NosqlBenchmark <TESTCASE> <DB>");
		
		validTestCasesString = "TestCases:";
		
		for (int i=0; i<testCases.size(); i++) {
			validTestCasesString += " ";
			validTestCasesString += testCases.get(i);
		}
		System.out.println("	" + validTestCasesString);
		
		validDBsString = "DBs:";
		
		for (int i=0; i<dbs.size(); i++) {
			validDBsString += " ";
			validDBsString += dbs.get(i);
		}
		
		System.out.println("	" + validDBsString);		
		System.exit(1);
	}

	/* Defining valid testCases and databases */
	public static void initialize() {
		testCases.add(TC_1);
		testCases.add(TC_2);		
		testCases.add(TC_3);
		dbs.add(DB_REDIS);
		dbs.add(DB_MONGODB);
		dbs.add(DB_MYSQL);

		SimpleFormatter fmt = new SimpleFormatter();
		StreamHandler sh = new StreamHandler(System.out, fmt);

		logger.addHandler(sh);
		logger.setUseParentHandlers(false);

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		initialize();

		if (args.length != 2) {
			System.err.println("Invalid parameter count!");
			usage();
		} else {
				if (!testCases.contains(args[0])) {
					System.err.println("Unkown <TESTCASE> selected!");
					usage();
				}
				if (!dbs.contains(args[1])) {
					System.err.println("Unknown <DB> selected!");
					usage();
				}

				NosqlBenchmark myNosqlBenchmark = new NosqlBenchmark(args[0], args[1]);

				myNosqlBenchmark.startBenchmark();
				
//				Document doc = XMLReader.getXMLFromFile("test1.xml");
//				JSON json = ConvertXMLToJSON.convertXmlToJson(doc);					
				System.out.println("Done!");
				System.exit(0);		

		}
	}

	public void startBenchmark() {
		DBClient dbClient = null;		
				
		if (dbName.equals(DB_REDIS)) {
			dbClient = new RedisClient();
		} else if (dbName.equals(DB_MONGODB)) {
			dbClient = new MongoClient();		

		} else if (dbName.equals(DB_MYSQL)) {
			dbClient = new MySqlClient();

		}
		else {
			usage();
		}
		
		TestCases.dbClient = dbClient;
		
		System.out.println(String.format("Executing TestCase \"%s\"!", testCaseName));
		
		TestCases.prepare(testCaseName);
		Date startDate = new Date();		
		TestCases.execute(testCaseName);
		Date endDate = new Date();
		
		long elapsedTime = endDate.getTime()-startDate.getTime(); 
		logger.info(String.format("Elapsed Time %d miliseconds", elapsedTime));
		System.out.println(String.format("  Elapsed Time %d miliseconds", elapsedTime));
		
	}

	public NosqlBenchmark(String testCaseName, String dbName) {

		this.testCaseName = testCaseName;
		this.dbName = dbName;		

		logger.info("\nTestCase = " + testCaseName + "\nDB       = " + dbName);
	}

}
