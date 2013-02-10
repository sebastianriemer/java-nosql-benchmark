package com.github.sebastianriemer.nosqlbenchmark;

import java.io.FileWriter;

import org.apache.commons.lang.UnhandledException;
import org.apache.log4j.*;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;

public class NosqlBenchmark {
	private static final Logger logger = Logger.getLogger(NosqlBenchmark.class
			.getName());;

	private String dbName;
	private String testCaseName;

	public final static String TC_1 = "TestAppendKeyValue";
	public final static String TC_2 = "TestUpdateKeyValue";
	public final static String TC_3 = "TestDeleteKeyValue";
	public final static String TC_4 = "TestSelectKeyValue";
	public final static String TC_5 = "TestAppendJSON";
	public final static String TC_6 = "TestUpdateJSON";
	public final static String TC_7 = "TestDeleteJSON";
	public final static String TC_8 = "TestSelectJSON";
	public final static String TC_9 = "TestConcurrentTransactions";
	public final static String TC_ALL = "All";

	private final static String DB_REDIS = "Redis";
	private final static String DB_MONGODB = "MongoDB";
	private final static String DB_MYSQL = "MySql";
	private final static String DB_CASSANDRA = "Cassandra";

	private final static int[] loadSizes = {1,2,5,10,20,50,75,100,250 /*100, 1000, 5000, 10000, 25000, 50000, 100000, 250000, 500000, 1000000*/};
	private final static long[] runTimes = {0,0,0,0,0,0,0,0,0 /*0, 0, 0, 0, 0, 0, 0, 0, 0, 0*/};

	private static ArrayList<String> testCases = new ArrayList<String>();
	private static ArrayList<String> dbs = new ArrayList<String>();

	/* Constructor */
	public NosqlBenchmark(String testCaseName, String dbName) {
		this.testCaseName = testCaseName;
		this.dbName = dbName;
	}

	/* Printing out usage of program */
	private static void usage() {
		String validTestCasesString;
		String validDBsString;
		logger.info("NosqlBenchmark <TESTCASE> <DB>");

		validTestCasesString = "Valid TestCases:";

		for (int i = 0; i < testCases.size(); i++) {
			validTestCasesString += " ";
			validTestCasesString += testCases.get(i);
		}
		logger.info(validTestCasesString);

		validDBsString = "Valid DBs:";

		for (int i = 0; i < dbs.size(); i++) {
			validDBsString += " ";
			validDBsString += dbs.get(i);
		}

		logger.info(validDBsString);
		System.exit(1);
	}

	/* Defining valid testCases and databases */
	private static void initialize() {
		testCases.add(TC_ALL);
		testCases.add(TC_1);
		testCases.add(TC_2);
		testCases.add(TC_3);
		testCases.add(TC_4);
		testCases.add(TC_5);
		testCases.add(TC_6);
		testCases.add(TC_7);
		testCases.add(TC_8);
		testCases.add(TC_9);		

		dbs.add(DB_REDIS);
		dbs.add(DB_MONGODB);
		dbs.add(DB_MYSQL);
		dbs.add(DB_CASSANDRA);

		PropertyConfigurator.configure("log4j.properties");
	}

	/**
	 * main method, argument handling, creating Benchmark
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		initialize();

		if (args.length != 2) {
			logger.error("Invalid parameter count!");
			usage();
		} else {
			if (!testCases.contains(args[0])) {
				logger.error("Unkown <TESTCASE> selected!");
				usage();
			}
			if (!dbs.contains(args[1])) {
				logger.error("Unknown <DB> selected!");
				usage();
			}

			NosqlBenchmark myNosqlBenchmark = new NosqlBenchmark(args[0],
					args[1]);

			myNosqlBenchmark.startBenchmark();

			logger.info("Benchmark finished!");
			System.exit(0);
		}
	}

	/*
	 * map testcase, execute test for each loadSize, write execution times to
	 * file
	 */
	private void startBenchmark() {		
		int exceptionCount = 0;
		
		
/*		DBClient testCassandraClient = getDbClient();
		testCassandraClient.firstTest();
*/		
		
		ArrayList<TestCase> testCases = getTestCases();
		for (TestCase testCase : testCases) {
			for (int i = 0; i < loadSizes.length; i++) {
				logger.info(String.format("Executing %s@%s with load=%d!",
						testCase.getName(), dbName, loadSizes[i]));
				//try {				
					runTimes[i] = execute(testCase, loadSizes[i]);
				/*} catch (Exception ex) {
					runTimes[i] = -1;
					logger.warn(String
							.format("The following exception occured during execution of %s@%s with load=%d!",
									testCase.getName(), dbName, loadSizes[i]));
					logger.error("Unfortunately for now, I don't know how to log the stack trace! :)", ex);			
					exceptionCount++;
				}*/
			}
			writeFile(testCase.getName());
		}
		
		if (exceptionCount > 0) {
			logger.warn("There have been a total of " + String.valueOf(exceptionCount) + " exception(s). You better check them out!");
		}
	}

	/* map dbName string to DBClient instance */
	public DBClient getDbClient() {
		DBClient dbClient = null;

		if (dbName.equals(DB_REDIS)) {
			dbClient = new RedisClient();
		} else if (dbName.equals(DB_MONGODB)) {
			dbClient = new MongoDBClient();
		} else if (dbName.equals(DB_MYSQL)) {
			dbClient = new MySqlClient();
		} else if (dbName.equals(DB_CASSANDRA)) {
			dbClient = new CassandraClient();
		} else {
			usage();
		}

		return dbClient;
	}

	/* map testCaseName string to TestCase instance */
	private ArrayList<TestCase> getTestCases() {
		ArrayList<TestCase> testCases = new ArrayList<TestCase>();

		if (testCaseName.equals(TC_1)) {
			testCases.add(new TestAppendKeyValue(getDbClient()));
		} else if (testCaseName.equals(TC_2)) {
			testCases.add(new TestUpdateKeyValue(getDbClient()));
		} else if (testCaseName.equals(TC_3)) {
			testCases.add(new TestDeleteKeyValue(getDbClient()));
		} else if (testCaseName.equals(TC_4)) {
			testCases.add(new TestSelectKeyValue(getDbClient()));
		} else if (testCaseName.equals(TC_5)) {
			testCases.add(new TestAppendJSON(getDbClient()));
		} else if (testCaseName.equals(TC_6)) {
			testCases.add(new TestUpdateJSON(getDbClient()));
		} else if (testCaseName.equals(TC_7)) {
			testCases.add(new TestDeleteJSON(getDbClient()));
		} else if (testCaseName.equals(TC_8)) {
			testCases.add(new TestSelectJSON(getDbClient()));
		} else if (testCaseName.equals(TC_9)) {
			testCases.add(new TestConcurrentTransactions(new DBClientFactory(getDbClient())));		
		} else if (testCaseName.equals(TC_ALL)) {
			testCases.add(new TestAppendKeyValue(getDbClient()));
			testCases.add(new TestUpdateKeyValue(getDbClient()));
			testCases.add(new TestDeleteKeyValue(getDbClient()));
			testCases.add(new TestSelectKeyValue(getDbClient()));
			testCases.add(new TestAppendJSON(getDbClient()));
			testCases.add(new TestUpdateJSON(getDbClient()));
			testCases.add(new TestDeleteJSON(getDbClient()));
			testCases.add(new TestSelectJSON(getDbClient()));
			//testCases.add(new TestConcurrentTransactions(new DBClientFactory(getDbClient())));
		} else {
			usage();
		}
		return testCases;

	}

	/* write runtimes for each loadSize of testCase into file */
	private void writeFile(String testCaseName) {
		PrintWriter out = null;	
		try {						
			out = new PrintWriter(new FileWriter("out/" + testCaseName + "_" + dbName + ".out"));
			out.println("#x y");
			
			for (int i = 0; i < loadSizes.length; i++) {
				out.println(loadSizes[i] + " " + runTimes[i]);
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		finally {
		      if (out != null) {		        
		        out.close();
		      }
		}
	}

	/* execute testCase, measure runTime */
	private long execute(TestCase testCase, int loadSize) throws UnhandledException {
		logger.debug("Start test ... ");

		logger.debug("Prepare ... ");
		
		testCase.prepare(loadSize);
		Date startDate = new Date();

		logger.debug("Execute ... ");
		testCase.execute();
		Date endDate = new Date();
		long elapsedTime = endDate.getTime() - startDate.getTime();
		logger.info(String.format("Elapsed Time %d miliseconds", elapsedTime));

		logger.debug("Cleanup ... ");
		testCase.cleanup();

		logger.debug("Test done!");

		return elapsedTime;

	}

}
