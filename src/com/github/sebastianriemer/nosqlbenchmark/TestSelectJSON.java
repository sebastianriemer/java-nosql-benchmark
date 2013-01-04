package com.github.sebastianriemer.nosqlbenchmark;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;

import net.sf.json.JSON;

public class TestSelectJSON implements TestCase {
	private static List<JSON> testJSONs;
	private DBClient dbClient;
	private static final Logger logger = Logger.getLogger(TestSelectJSON.class
			.getName());

	private String name = new String(this.getClass().getSimpleName());
	
	/* Getter for name */
	public String getName() {
		return name;
	}

	
	/* set dbClient */
	public TestSelectJSON(DBClient dbClient) {
		this.dbClient = dbClient;
	}

	/* initialize xml files, depending on loadSize */
	@Override
	public void prepare(int loadSize) {
		logger.debug("Preparing xml files ...");
		initialize_xmlfiles(loadSize);
		if (dbClient != null) {
			dbClient.openConnection();

			for (int i = 0; i < testJSONs.size(); i++) {						
				dbClient.append(Integer.toString(i), testJSONs.get(i));				
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
			for (int i = 0; i < testJSONs.size(); i++) {				
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

	/*
	 * load xmlfiles from directory and append to ArrayList Currently, 10000 xml
	 * files are available and if more are required the same files will be added
	 * to ArrayList
	 */
	public static void initialize_xmlfiles(int size) {
		testJSONs = new ArrayList<JSON>();
		int i = 0;

		URL url = TestCases.class.getResource("resources/xmlfiles");
		try {
			File dir = new File(url.toURI());
			int fileCount = dir.listFiles().length;
			logger.debug("Size is " + String.valueOf(size) + ", FileCount is "
					+ String.valueOf(fileCount));
			while (size > 0) {
				for (File child : dir.listFiles()) {
					testJSONs.add(ConvertXMLToJSON.convertXmlToJson(child
							.getName()));
					i++;
					if (i > size)
						break;
				}
				size = size - fileCount;
			}

		} catch (URISyntaxException e) {
			e.printStackTrace();
		}

	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
		throw new NotImplementedException();
	}
}
