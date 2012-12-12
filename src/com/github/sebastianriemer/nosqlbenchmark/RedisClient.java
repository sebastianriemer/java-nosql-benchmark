package com.github.sebastianriemer.nosqlbenchmark;

import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;

import net.sf.json.JSON;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class RedisClient implements DBClient {
	private JedisPool pool;
	private Jedis jedis;
	private static final Logger logger = Logger.getLogger(RedisClient.class.getName());
	
	public void append(String key, JSON jsonObj)
	{
		try {
			 jedis.set(key, jsonObj.toString());
			} finally {
			  /// ... it's important to return the Jedis instance to the pool once you've finished using it
			  pool.returnResource(jedis);
			}
	}
	
	public void append(String key, String value) {
		try {
			 jedis.set(key, value);
			} finally {
			  /// ... it's important to return the Jedis instance to the pool once you've finished using it
			  pool.returnResource(jedis);
			}
				
	}
	public String get(String key) {
		String returnValue = new String();
		try {
			returnValue = jedis.get(key);			  			
			} finally {
			  /// ... it's important to return the Jedis instance to the pool once you've finished using it
			  pool.returnResource(jedis);			  
			}		 
		return returnValue;
	}
	
	public void openConnection() {
		try {
		 pool = new JedisPool(new JedisPoolConfig(), "localhost");
		 jedis = pool.getResource();
		} catch (JedisConnectionException ex) {
			logger.logp(Level.SEVERE, RedisClient.class.toString(),
					"openConnection",
					String.format("Could not connect to server! Check that server is running ..."));
			throw(ex);
			
		}
	}
	public void closeConnection() {
		pool.destroy();
	}
	
	public RedisClient() {				
		SimpleFormatter fmt = new SimpleFormatter();
		StreamHandler sh = new StreamHandler(System.out, fmt);
		
		logger.addHandler(sh);
		
	}
	
	public void firstTest() {
		try {
			  /// ... do stuff here ... for example
			logger.logp(Level.FINE, RedisClient.class.toString(),
					"openConnection",
					String.format("Setting key 'aktion2' = 'bimsen' ..."));
			
			 jedis.set("aktion2", "bimsen");
			  String returnValue = jedis.get("aktion2");
			  
			  logger.logp(Level.FINE, RedisClient.class.toString(),
						"openConnection",
						String.format("Retrieving key 'aktion2' ..."));
							  
			  if (returnValue != null) {
				  logger.logp(Level.FINE, RedisClient.class.toString(),
							"openConnection",
							String.format("Got value '%s'", returnValue));									  				 
			  }
			/*  jedis.zadd("sose", 0, "car"); jedis.zadd("sose", 0, "bike"); 
			  Set<String> sose = jedis.zrange("sose", 0, -1);*/
			} finally {
			  /// ... it's important to return the Jedis instance to the pool once you've finished using it
			  pool.returnResource(jedis);
			}
	}
}
