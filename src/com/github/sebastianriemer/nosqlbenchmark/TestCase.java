package com.github.sebastianriemer.nosqlbenchmark;

import org.apache.commons.lang.UnhandledException;

public interface TestCase extends Runnable {		
	
	public String getName();
	
	public void prepare(int loadSize) throws UnhandledException;
	
	public void execute();
		
	public void cleanup();
}
