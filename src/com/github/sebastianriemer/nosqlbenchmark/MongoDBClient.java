package com.github.sebastianriemer.nosqlbenchmark;

import java.net.UnknownHostException;

import org.apache.log4j.Logger;

import net.sf.json.JSON;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class MongoDBClient implements DBClient {
	private static final Logger logger = Logger.getLogger(MongoDBClient.class.getName());
	private Mongo mongo;
	private DB db;
	private DBCollection collection;

	/* append JSON object with given key */
	public void append(String key, JSON jsonObj) {
		DBObject dbObj = (DBObject) com.mongodb.util.JSON.parse(jsonObj.toString());
		BasicDBObject doc = new BasicDBObject();
		doc.put("key", key);
		doc.put("value", dbObj);
		collection.insert(doc);
	}

	/* append string with the given key */
	public void append(String key, String value) {		
		BasicDBObject document = new BasicDBObject();
		document.put("key", key);
		document.put("value", value);
		collection.insert(document);
	}

	/* return the value of the given key */
	public String get(String key) {
		/*
		 * ToDo: Currently, the returned Value is actually the whole document
		 * instead of that, return only the value of the field "value"
		 */
		BasicDBObject searchQuery = new BasicDBObject(); 
		searchQuery.put("key", key);
		DBCursor cursor = collection.find(searchQuery); 

		if (cursor.hasNext())
			return cursor.next().toString();
		
		return null;
	}

	/* delete all data from db */
	public void cleanup() {
		collection.drop();
	}

	/* open a connection */
	public void openConnection() {
		try {
			logger.info(String
					.format("Opening Connection to localhost on port 27017 ..."));
			mongo = new Mongo("localhost", 27017);

			db = mongo.getDB("local");
			collection = db.getCollection("things");

		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
	}

	/* close the connection */
	public void closeConnection() {
		logger.info(String.format("Closing Connection ..."));
	}

	/* initial testing method */
	public void firstTest() {
		logger.info(String
				.format("Creating message with tags 'id' and 'msg' ..."));

		BasicDBObject doc = new BasicDBObject();
		doc.put("msg", "hello world mongoDB in Java");

		DBCursor cursor = collection.find(doc);
		logger.info(String.format("Reading from cursor ..."));

		while (cursor.hasNext()) {
			logger.info(String.format("-> %s", cursor.next()));
		}

		logger.info(String.format("Done!"));
	}

	/* initial JSON testing method */
	public void secondTestJSON(JSON json) {
		/*
		 * this feels kinda weird, two different classes named JSON, is it
		 * really necessary to do like this?
		 */

		DBObject jsonObj = (DBObject) com.mongodb.util.JSON.parse(json
				.toString());
		BasicDBObject doc = new BasicDBObject();
		doc.put("key", "50001");
		doc.put("value", jsonObj);
		logger.debug("Inserting wrapped jsonObj into db ...");
		collection.insert(doc);
		logger.debug("Done!");
	}

	/* delete key */
	@Override
	public void delete(String key) {
		BasicDBObject document = new BasicDBObject();
		document.put("key", key);
		collection.remove(document); 				
	}

	@Override
	public String getJSON(String key) {	
		return get(key);
	}

}
