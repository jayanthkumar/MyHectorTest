package com.cassandra.services;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

import com.cassandra.constants.Constants;

import me.prettyprint.cassandra.model.BasicColumnDefinition;
import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.FailoverPolicy;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnIndexType;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;

/**
 * 
 * @author Jayanth Kumar
 *
 */
public class CommonService {
	private Cluster clusterObj;
	private static final Logger log = Logger.getLogger(CommonService.class);
	public CommonService(Cluster clusterObj) {
		this.clusterObj = clusterObj;
	}
	
	/**
	 * Creates Keyspace if doesn't exist
	 * @param ksName Keyspace Name 
	 * @return Returns KeyspaceDefinition Object 
	 */
	public KeyspaceDefinition createKeySpace(String ksName){
		KeyspaceDefinition keySpaceDefObj = clusterObj.describeKeyspace(ksName);
		// Check to see if Keyspace is already existed or not
		if(keySpaceDefObj == null){	
			KeyspaceDefinition ksObj = HFactory.createKeyspaceDefinition(Constants.KEYSPACE_NAME, Constants.STRATEGYCLASS, Constants.REPLICATIONFACTOR, null);
			clusterObj.addKeyspace(ksObj, false);
		}
		keySpaceDefObj = clusterObj.describeKeyspace(Constants.KEYSPACE_NAME);
		if(keySpaceDefObj != null)
			log.info("Keyspace is created ");
		return keySpaceDefObj;
	}
	
	public Keyspace getKeyspace(String keyspaceName, Cluster cluster, ConsistencyLevelPolicy consistencyLevel,
		       FailoverPolicy failoverPolicy, Map<String, String> credentials) {
		return HFactory.createKeyspace(Constants.KEYSPACE_NAME, clusterObj, consistencyLevel, failoverPolicy, credentials);
	}
	
	public Keyspace getKeyspace(String keyspaceName, Cluster cluster, ConsistencyLevelPolicy consistencyLevel) {
		return HFactory.createKeyspace(Constants.KEYSPACE_NAME, clusterObj, consistencyLevel);
	}
	
	/**
	 * To drop an existed Keyspace
	 * @param ksName The name of the keyspace
	 */
	public void dropKeySpace(String ksName) {
		KeyspaceDefinition keySpaceDefObj = clusterObj.describeKeyspace(ksName);
		if(keySpaceDefObj != null) {
			clusterObj.dropKeyspace(keySpaceDefObj.getName());
			log.info(ksName+" Keyspace is dropped");
		}
	}
	
	/**
	 * Creates Column Family for the specified keyspace 
	 * @param keyspaceName The name of the keyspace
	 * @param cfName The name of the column family
	 * @param superType The type of the column family .Create column family of type Super if this parameter is set to true
	 * @throws HectorException
	 */
	public void createColumnFamily(String keyspaceName, String cfName, boolean superType) throws HectorException {
		ColumnFamilyDefinition cf = HFactory.createColumnFamilyDefinition(keyspaceName, cfName);
		if(superType) {
			cf.setColumnType(ColumnType.SUPER);
		}
		clusterObj.addColumnFamily(cf);
		if(clusterObj.describeKeyspace(keyspaceName).getCfDefs().contains(cf))
			log.info("Column Family is created : "+cfName);
	}
	
	/**
	 * To create a column family with an index
	 * @param keyspaceName The name of the keyspace
	 * @param cfName The name of the column family
	 * @param index_name The name of the index
	 * @param indexType The type of the index
	 * @param validationClass Validation class of the indexed column
	 *//*depricated
	public void createIndexexColumnFamily(String keyspaceName, String cfName, String index_name, ColumnIndexType indexType, String validationClass) {
		ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keyspaceName, cfName);
		BasicColumnFamilyDefinition columnFamilyDefinition = new BasicColumnFamilyDefinition(cfDef);

		BasicColumnDefinition columnDefinition = new BasicColumnDefinition();
		columnDefinition.setName(StringSerializer.get().toByteBuffer(index_name));
		columnDefinition.setIndexName(index_name);
		columnDefinition.setIndexType(indexType);
		columnDefinition.setValidationClass(validationClass);

		columnFamilyDefinition.addColumnDefinition(columnDefinition);

		clusterObj.addColumnFamily(new ThriftCfDef(columnFamilyDefinition));
	}*/
	
	/**
	 * Updates Column Family
	 * @param keyspaceName The name of the keyspace to which column family belongs
	 * @param cfDef The columnFamilyDefination object to be updated
	 */
	public void updateColumnFamily(String keyspaceName, ColumnFamilyDefinition cfDef) {
		ColumnFamilyDefinition foundColumnFamilyDefinition = getColumnFamily(keyspaceName, cfDef.getName());
		if (foundColumnFamilyDefinition == null) {
			log.info("Could not find columnFamilyDefinition with name " + cfDef.getName());
		} else {
			clusterObj.updateColumnFamily(foundColumnFamilyDefinition);
			log.info("Updated ColumnFamily : "+cfDef.getName());
		}
	}
	
	/**
	 * To get Column family Definition Object
	 * @param keyspaceName The name of the keyspace to which column family belongs
	 * @param cfName The name of the column family 
	 * @return Returns ColumnFamilyDefinition object for the specified column family name of a specified keyspace
	 */
	public ColumnFamilyDefinition getColumnFamily(String keyspaceName, String cfName){
		List<ColumnFamilyDefinition> columnFamilyDefinitions = clusterObj.describeKeyspace(keyspaceName).getCfDefs();
		ColumnFamilyDefinition foundColumnFamilyDefinition = null;
		for (ColumnFamilyDefinition columnFamilyDefinition : columnFamilyDefinitions) {
			if (cfName.equals(columnFamilyDefinition.getName())) {
				foundColumnFamilyDefinition = columnFamilyDefinition;
			}
		}
		if(foundColumnFamilyDefinition != null) {
			log.info("Getting ColumnFamily :"+foundColumnFamilyDefinition.getName());
		}
		return foundColumnFamilyDefinition;
	}
	
	/**
	 * Deletes column family
	 * @param keySpaceName The name of the keyspace 
	 * @param cfName The name of the column family
	 */
	public void deleteColumnFamily(String keySpaceName, String cfName) {
		clusterObj.dropColumnFamily(keySpaceName, cfName);
		if(null == getColumnFamily(keySpaceName,cfName)){
			log.info("Deleted Column Family :"+cfName);
		}
	}
	
	/**
	 * To Delete a record or row
	 * @param rowKey The row key of the record
	 * @param cfName The name of the column family in which record exist
	 */
	public void deleteRecord(String rowKey, String cfName){
		Keyspace keySpace = HFactory.createKeyspace(Constants.KEYSPACE_NAME, clusterObj);
		Mutator<String> mutator = HFactory.createMutator(keySpace, Constants.STRING_SERIALIZER);
		mutator.addDeletion(rowKey, cfName);
		MutationResult mutationResult = mutator.execute();
		log.info("Delete record of key : "+rowKey+" from column family of "+cfName+" :"+mutationResult.toString());
	}
	
	/**
	 * Is an expensive operation . Used to shut Down the cluster
	 */
	public void shutDownCluster(){
		HFactory.shutdownCluster(clusterObj);
	}

}
