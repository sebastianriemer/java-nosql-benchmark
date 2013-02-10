package com.github.sebastianriemer.nosqlbenchmark;

import java.util.Arrays;


import org.apache.log4j.Logger;

import net.sf.json.JSON;


import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;

public class CassandraClient implements DBClient {
	private static final String C_KEYSPACE = "testKeySpace";
	private static final String C_COLUMNFAMILY = "testColumnFamily";

	private static final Logger logger = Logger.getLogger(CassandraClient.class
			.getName());

	private Cluster myCluster = HFactory.getOrCreateCluster("test-cluster",
			"localhost:9160");

	private ColumnFamilyTemplate<String, String> template;

	@Override
	public void append(String key, JSON jsonObj) {
		ColumnFamilyUpdater<String, String> updater = template
				.createUpdater(key);
		// TODO: check whether toString is sufficient
		updater.setString("value", jsonObj.toString());
		try {
			template.update(updater);
		} catch (HectorException e) {
			logger.error("Exception while testing update!");
			throw e;
		}

	}

	@Override
	public void append(String key, String value) {
		ColumnFamilyUpdater<String, String> updater = template
				.createUpdater(key);
		updater.setString("value", value);
		try {
			template.update(updater);
		} catch (HectorException e) {
			logger.error("Exception while testing update!");
			throw e;
		}
	}

	@Override
	public void delete(String key) {
		try {
			template.deleteColumn(key, "value");
		} catch (HectorException e) {
			logger.error("Exception while deleting value of key =" + key + "!");
			throw e;
		}
	}

	@Override
	public String get(String key) {

		try {
			ColumnFamilyResult<String, String> res = template.queryColumns(key);
			return res.getString("value");
		} catch (HectorException e) {
			logger.error("Exception while trying to get value of key= " + key
					+ "!");
		}
		return null;
	}

	@Override
	public void cleanup() {				
		//myCluster.dropKeyspace(C_KEYSPACE);
		myCluster.truncate(C_KEYSPACE, C_COLUMNFAMILY);

	}

	@Override
	public void openConnection() {				
		KeyspaceDefinition keyspaceDef = myCluster.describeKeyspace(C_KEYSPACE);
		
		if (keyspaceDef == null) 
			createSchema();	
		
		
		Keyspace ksp = HFactory.createKeyspace(C_KEYSPACE, myCluster);			
			
		template = new ThriftColumnFamilyTemplate<String, String>(ksp,
				C_COLUMNFAMILY, StringSerializer.get(), StringSerializer.get());		
	}

	private void createSchema() {		
		ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(
				C_KEYSPACE, C_COLUMNFAMILY, ComparatorType.BYTESTYPE);		

		KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(
				C_KEYSPACE, ThriftKsDef.DEF_STRATEGY_CLASS, 1,
				Arrays.asList(cfDef));			
		
		myCluster.addKeyspace(newKeyspace, true);		
	}

	@Override
	public void firstTest() {
		// <String, String> correspond to key and Column name.
		ColumnFamilyUpdater<String, String> updater = template
				.createUpdater("12345");
		updater.setString("value", "www.datastax.com");
		// updater.setLong("time", System.currentTimeMillis());

		try {
			template.update(updater);
		} catch (HectorException e) {
			logger.error("Exception while testing update!");
			throw e;
		}

		try {
			ColumnFamilyResult<String, String> res = template
					.queryColumns("12345");
			String value = res.getString("value");
			// value should be "www.datastax.com" as per our previous insertion.
			logger.debug("Result of read-test: " + value);
		} catch (HectorException e) {
			logger.error("Exception while testing read!");
			throw e;
		}
		try {
			template.deleteColumn("12345", "value");
		} catch (HectorException e) {
			logger.error("Exception while testing delete!");
			throw e;
		}
	}

	@Override
	public void closeConnection() {
		// TODO Auto-generated method stub

	}

}
