package com.github.sebastianriemer.nosqlbenchmark;

import java.util.Arrays;
import java.util.List;

import net.sf.json.JSON;


import me.prettyprint.cassandra.model.BasicColumnDefinition;
import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftCfDef;


import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnIndexType;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
public class CassandraClient implements DBClient {
	private static final String DYN_KEYSPACE = "DynamicKeyspace";
	private static final String DYN_CF = "DynamicCf";
	private static final String CF_SUPER = "SuperCf";
	    
	private static StringSerializer stringSerializer = StringSerializer.get();
	    
	@Override
	public void append(String key, JSON jsonObj) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void append(String key, String value) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void delete(String key) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String get(String key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void openConnection() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void firstTest() {
		 Cluster cluster = HFactory.getOrCreateCluster("TestCluster", "localhost:9160");
         
	        try {
	            if ( cluster.describeKeyspace(DYN_KEYSPACE) != null ) {
	              cluster.dropKeyspace(DYN_KEYSPACE);
	            }
	            
	            BasicColumnDefinition columnDefinition = new BasicColumnDefinition();
	            columnDefinition.setName(stringSerializer.toByteBuffer("birthdate"));
	            columnDefinition.setIndexName("birthdate_idx");
	            columnDefinition.setIndexType(ColumnIndexType.KEYS);
	            columnDefinition.setValidationClass(ComparatorType.LONGTYPE.getClassName());
	            
	            BasicColumnFamilyDefinition columnFamilyDefinition = new BasicColumnFamilyDefinition();
	            columnFamilyDefinition.setKeyspaceName(DYN_KEYSPACE);
	            columnFamilyDefinition.setName(DYN_CF);
	            columnFamilyDefinition.addColumnDefinition(columnDefinition);
	            
	            BasicColumnFamilyDefinition superCfDefinition = new BasicColumnFamilyDefinition();
	            superCfDefinition.setKeyspaceName(DYN_KEYSPACE);
	            superCfDefinition.setName(CF_SUPER);
	            superCfDefinition.setColumnType(ColumnType.SUPER);
	            
	            
	            
	            ColumnFamilyDefinition cfDefStandard = new ThriftCfDef(columnFamilyDefinition);
	            ColumnFamilyDefinition cfDefSuper = new ThriftCfDef(superCfDefinition);
	            
	            KeyspaceDefinition keyspaceDefinition = 
	                HFactory.createKeyspaceDefinition(DYN_KEYSPACE, "org.apache.cassandra.locator.SimpleStrategy", 
	                    1, Arrays.asList(cfDefStandard, cfDefSuper));
	                                               
	            cluster.addKeyspace(keyspaceDefinition);
	            
	            // insert some data
	            
	            List<KeyspaceDefinition> keyspaces = cluster.describeKeyspaces();
	            for (KeyspaceDefinition kd : keyspaces) {
	                if ( kd.getName().equals(DYN_KEYSPACE) ) {
	                    System.out.println("Name: " +kd.getName());
	                    System.out.println("RF: " +kd.getReplicationFactor());
	                    System.out.println("strategy class: " +kd.getStrategyClass());
	                    List<ColumnFamilyDefinition> cfDefs = kd.getCfDefs();
	                    for (ColumnFamilyDefinition def : cfDefs) {
	                      System.out.println("  CF Type: " +def.getColumnType());
	                      System.out.println("  CF Name: " +def.getName());
	                      System.out.println("  CF Metadata: " +def.getColumnMetadata());  
	                    }
	                    
	                    
	                } 
	            }
	            /* I think I did not yet insert anything, let's try to extend the code found here 
	             * 
	             * http://hector-client.github.com/hector/build/html/content/getting_started.html
	             * */
	            
	            /*Keyspace ksp = HFactory.createKeyspace(DYN_KEYSPACE, cluster);
	            ColumnFamilyTemplate<String, String> template =
                        new ThriftColumnFamilyTemplate<String, String>(ksp,
                                                                       columnFamily,
                                                                       StringSerializer.get(),
                                                                       StringSerializer.get());*/
	            
	            
	        } catch (HectorException he) {
	            he.printStackTrace();
	        }
	        cluster.getConnectionManager().shutdown(); 
	    }
	

	@Override
	public void closeConnection() {
		// TODO Auto-generated method stub
		
	}

}
