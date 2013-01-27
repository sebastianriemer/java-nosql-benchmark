package com.github.sebastianriemer.nosqlbenchmark;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.UnhandledException;
import org.apache.log4j.Logger;

public class TestConcurrentTransactions implements TestCase {
	
	/* This constant is multiplied with the thread nr and then added to each keyId
	 * which will make each record identifiable to its thread 
	 *  */
	private static int C_SCALE_SESSION_ID = 10000;
	private DBClientFactory dbClientFactory;
	private static final Logger logger = Logger.getLogger(TestConcurrentTransactions.class
			.getName());

	private String name = new String(this.getClass().getSimpleName());
	private List<TestCase> testCases;
	
	/* Getter for name */
	@Override
	public String getName() {
		return name;
	}

	/* set dbClient */
	public TestConcurrentTransactions(DBClientFactory dbClientFactory) {
		this.dbClientFactory = dbClientFactory;		
	}

	/* generate dbClients, and open connections */
	/* in this context, loadSize determines the number of connections */
	@Override
	public void prepare(int loadSize) throws UnhandledException {				
		try {
			testCases = new ArrayList<TestCase>();
			for (int i=0; i<loadSize; i++)		{
				/* to be able to distinguish the inserts of the different threads, a message prefix is added */				
				testCases.add(new TestUserTransaction(dbClientFactory.createNewInstance(), i*C_SCALE_SESSION_ID));
				testCases.get(i).prepare(100);
			}
		}
		catch (UnknownDBClientTypeException ex) {
			logger.error("Unknown type of DBClient found in TestConcurrentKeyValue.prepare!", ex);
			throw new UnhandledException(ex);
		}
		
	}

	/* execute test */
	@Override
	public void execute() {		
		/*for (TestCase testCase: testCases) {
			Thread t = new Thread(testCase);
			t.start();					
		}		*/
		 ExecutorService executor = Executors.newFixedThreadPool(testCases.size());
		 for (TestCase testCase: testCases) {
			 logger.info("Starting new thread ...");
		     executor.execute(testCase);
		    }
		    // This will make the executor accept no new threads
		    // and finish all existing threads in the queue
		    executor.shutdown();
		   
		    // Wait until all threads are finish
		    while (!executor.isTerminated()) {		    	
		    	try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					logger.warn("Main thread got interrupt, wtf?!");
					throw new UnhandledException(e);
				}
		    	logger.info("waiting for threads to finish ...");
		    }
		    logger.info("All threads complete!");
		
			
	}

	/* cleanup database */
	@Override
	public void cleanup() {
		DBClient dbClient;
		try {
			dbClient = dbClientFactory.createNewInstance();
			dbClient.openConnection();
			dbClient.cleanup();
			dbClient.closeConnection();
		} catch (UnknownDBClientTypeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	/* is this the correct way? */
	@Override
	public void run() {
		execute();
		
	}
	
}
