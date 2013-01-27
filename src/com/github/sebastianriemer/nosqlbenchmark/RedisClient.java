package com.github.sebastianriemer.nosqlbenchmark;

import org.apache.log4j.Logger;

import net.sf.json.JSON;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class RedisClient implements DBClient {
	private JedisPool pool;
	private Jedis jedis;
	private static final Logger logger = Logger.getLogger(RedisClient.class
			.getName());

	/* append JSON object with given key */
	public void append(String key, JSON jsonObj) {
		try {
			jedis.set(key, jsonObj.toString());			
		} finally {
			pool.returnResource(jedis);
		}
	}

	/* append string with the given key */
	public void append(String key, String value) {
		try {
			jedis.set(key, value);
		} finally {
			pool.returnResource(jedis);
		}

	}

	/* return the value of the given key */
	public String get(String key) {
		String returnValue = new String();
		try {
			returnValue = jedis.get(key);
		} finally {
			pool.returnResource(jedis);
		}
		return returnValue;
	}

	/* delete all data from db */
	public void cleanup() {
		jedis.flushDB();
	}

	/* open a connection */
	public void openConnection() {
		try {
			logger.debug(String.format("Opening Connection to localhost ..."));
			pool = new JedisPool(new JedisPoolConfig(), "localhost");
			jedis = pool.getResource();
		} catch (JedisConnectionException ex) {
			logger.error("Could not connect to server! Check that server is running ...");
			throw (ex);

		}
	}

	/* close the connection */
	public void closeConnection() {
		logger.debug(String.format("Closing Connection ..."));
		pool.destroy();
	}

	/* initial testing method */
	public void firstTest() {
		try {
			jedis.set("someKey", "someValue");
			String returnValue = jedis.get("aktion2");
			logger.debug("Got value " + returnValue);

		} finally {
			pool.returnResource(jedis);
		}
	}

	/* remove key */
	@Override
	public void delete(String key) {
		try {
			jedis.del(key);			
		} finally {
			pool.returnResource(jedis);
		}
		
	}

}
