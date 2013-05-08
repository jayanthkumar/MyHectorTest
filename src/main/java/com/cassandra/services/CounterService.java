package com.cassandra.services;

import org.apache.cassandra.db.CounterColumn;

import com.cassandra.constants.Constants;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.CounterQuery;
import me.prettyprint.hector.api.query.QueryResult;

/**
 * 
 * @author Jayanth Kumar
 *
 */
public class CounterService {
	/**
	 * To create Counter Column Family
	 * @param cluster Cluster object
	 * @param cfName The Column Family name
	 * @param isSuper Should be true to create super column family
	 */
	public void createCounterColumnFamily(Cluster cluster, String cfName, boolean isSuper) {
		ColumnFamilyDefinition cfd = HFactory.createColumnFamilyDefinition(
				Constants.KEYSPACE_NAME, cfName);
		if(isSuper) {
			cfd.setColumnType(ColumnType.SUPER);
		} else {
			cfd.setColumnType(ColumnType.STANDARD);
		}
		cfd.setDefaultValidationClass(ComparatorType.COUNTERTYPE.getClassName());
		cluster.addColumnFamily(cfd);
	}
	
	/**
	 * To insert a Counter column 
	 * @param keySpace The keyspace object
	 * @param cf The column family name
	 * @param key Unique row key 
	 * @param name The name of the counter column
	 * @param value The value of the counter column
	 */
	public void insertCounterColumn(Keyspace keySpace, String cf, String key,
			String name, long value) {
		Mutator<String> mutator = HFactory.createMutator(keySpace,
				Constants.STRING_SERIALIZER);
		HCounterColumn<String> cc = HFactory.createCounterColumn(name, value,
				Constants.STRING_SERIALIZER);
		mutator.insertCounter(key, cf, cc);
		MutationResult mr = mutator.execute();
		System.out.println(mr.toString());
	}
	
	/**
	 * To get Counter column
	 * @param keyspace The keyspace object
	 * @param cf The column family name
	 * @param key Unique row key 
	 * @param columnName The counter column name
	 * @return
	 */
	public HCounterColumn<String> getCounterColumn(Keyspace keyspace,
			String cf, String key, String columnName) {
		CounterQuery<String, String> cq = HFactory.createCounterColumnQuery(
				keyspace, Constants.STRING_SERIALIZER,
				Constants.STRING_SERIALIZER);
		cq.setColumnFamily(cf);
		cq.setKey(key);
		cq.setName(columnName);
		QueryResult<HCounterColumn<String>> qr = cq.execute();
		return qr.get();
	}

	/** 
	 * To Increment a counter column by 1
	 * @param ks The keyspace object
	 * @param cfName The column family name
	 * @param key Unique row key
	 * @param columnName The counter column name
	 */
	public void incrementCounter(Keyspace ks, String cfName, String key,
			String columnName) {
		Mutator<String> mutator = HFactory.createMutator(ks,
				Constants.STRING_SERIALIZER);
		mutator.incrementCounter(key, cfName, columnName, 1l);
	}
	
	/**
	 * To decrement a counter column by 1
	 * @param ks The keyspace object
	 * @param cfName The column family name
	 * @param key Unique row key
	 * @param columnName The counter column name
	 */
	public void decrementCounter(Keyspace ks, String cfName, String key,
			String columnName) {
		Mutator<String> mutator = HFactory.createMutator(ks,
				Constants.STRING_SERIALIZER);
		mutator.decrementCounter(key, cfName, columnName, 1l);
	}
	
	/**
	 * To delete a counter column
	 * @param ks The keyspace object
	 * @param cfName The column family name
	 * @param key Unique row key
	 * @param columnName The counter column name
	 */
	public void deleteCounterColumn(Keyspace ks, String cfName, String key,
			String columnName) {
		Mutator<String> mutator = HFactory.createMutator(ks,
				Constants.STRING_SERIALIZER);
		mutator.deleteCounter(key, cfName, columnName,
				Constants.STRING_SERIALIZER);
	}
}
