package com.github.sebastianriemer.nosqlbenchmark;

public class DBClientFactory {

	private DBClient dbClient;
	
	public DBClientFactory(DBClient dbClient) {
		this.dbClient = dbClient;
	}
	
	
	public DBClient createNewInstance() throws UnknownDBClientTypeException {
		if (dbClient instanceof RedisClient) 
			return new RedisClient();		
		else if (dbClient instanceof MongoDBClient)			
			return new MongoDBClient();
		else if (dbClient instanceof MySqlClient)
			return new MySqlClient();
		else
			throw new UnknownDBClientTypeException("Unknown DBClient found in DBClientFactory!");
	}
}
