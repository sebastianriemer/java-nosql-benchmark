package com.github.sebastianriemer.nosqlbenchmark;

import net.sf.json.JSON;

interface DBClient {
	
	public void append(String key, JSON jsonObj);
	public void append(String key, String value);
	
	public void delete(String key);
	
	public String get(String key);
	
	public String getJSON(String key);
	
	public void cleanup();
	
	public void openConnection();
	public void firstTest();
	public void closeConnection();
	
	
	
}
