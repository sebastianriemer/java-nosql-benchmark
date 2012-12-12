package com.github.sebastianriemer.nosqlbenchmark;

import java.util.logging.Logger;

import java.net.UnknownHostException;

import net.sf.json.JSON;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class MongoClient implements DBClient {
	private static final Logger logger =  Logger.getLogger(MongoClient.class.getName());
	private Mongo mongo;
	private DB db;
	private DBCollection collection;

	public void append(String key, JSON jsonObj) {
		DBObject dbObj = (DBObject) com.mongodb.util.JSON.parse(jsonObj.toString());
		BasicDBObject doc = new BasicDBObject();
		doc.put("key", key);		
		doc.put("value", dbObj);		
		collection.insert(doc);			
	}
	
	public void append(String key, String value) {
		try {			
			
			BasicDBObject document = new BasicDBObject();	// create a document to store key and value
		
			document.put("key", key);
			document.put("value", value);		


			collection.insert(document);			// save it into collection named "yourCollection"
			
		} finally {
		}
	}

	public String get(String key) {				
			/* ToDo: Currently, the returned Value is actually the whole document
			 * instead of that, return only the value of the field "value" */
			BasicDBObject searchQuery = new BasicDBObject(); // create query
			searchQuery.put("key", key);
			
			DBCursor cursor = collection.find(searchQuery);  // query it						
			
			if (cursor.hasNext()) 				
				return cursor.next().toString();				
			
		return null;
	}

	public void openConnection() {
		try {			
			logger.info(String.format("Opening Connection to localhost on port 27017 ..."));
			mongo = new Mongo("localhost", 27017);

			// connect to mongoDB, ip and port number
			// get database from MongoDB,
			// if database doesn't exists, mongoDB will create it automatically
			db = mongo.getDB("local");

			// Get collection from MongoDB, database named "yourDB"
			// if collection doesn't exists, mongoDB will create it
			// automatically
			collection = db.getCollection("things");
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}

	}

	public void closeConnection() {
		logger.info(String.format("Closing Connection ..."));
	}

	public MongoClient() {		
		/*SimpleFormatter fmt = new SimpleFormatter();
		StreamHandler sh = new StreamHandler(System.out, fmt);
		
		logger.addHandler(sh);*/		
		
		logger.info("Parent of this logger = " +logger.getParent().getName());
		//logger.setUseParentHandlers(false);
	}

	public void firstTest() {
		try {
			
			logger.info(String.format("Creating message with tags 'id' and 'msg' ..."));	

			//BasicDBObject document = new BasicDBObject();			// create a document to store key and value
			
			/*document.put("id", 1001);
			document.put("msg", "Test Message " + new Date().toString());
			document.put("msg2", "2nd Test Message " + new Date().toString());*/


			// collection.insert(document);			// save it into collection named "yourCollection"


			BasicDBObject doc = new BasicDBObject();					//
			doc.put("msg", "hello world mongoDB in Java");

			
			DBCursor cursor = collection.find(doc);  // query it

			// loop over the cursor and display the retrieved result
			logger.info(String.format("Reading from cursor ..."));		
			
			while (cursor.hasNext()) {
				logger.info(String.format("-> %s", cursor.next()));				
			}
			
			logger.info(String.format("Done!"));

		} finally {

		}
	}
	
	public void secondTestJSON(JSON json) {		
		/* this feels kinda weird, two different classes named JSON, is it really necessary to do like this? */
		
		DBObject jsonObj = (DBObject) com.mongodb.util.JSON.parse(json.toString());
		BasicDBObject doc = new BasicDBObject();
		doc.put("key", "50001");		
		doc.put("value", jsonObj);
		System.out.println("Inserting wrapped jsonObj into db ...");
		collection.insert(doc);	
		System.out.println("Done!");
	}
	
}
