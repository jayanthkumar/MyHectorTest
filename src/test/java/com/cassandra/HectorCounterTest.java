package com.cassandra;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.factory.HFactory;

import org.junit.BeforeClass;
import org.junit.Test;

import com.cassandra.constants.Constants;
import com.cassandra.services.CommonService;
import com.cassandra.services.CounterService;
/**
 * 
 * @author Jayanth Kumar.S
 *
 */
public class HectorCounterTest {
	public static String cfName = "Counters";
	private static Cluster cluster;

	@BeforeClass
	public static void initialize() {
		cluster = HFactory.getOrCreateCluster(Constants.CLUSTERNAME,
				Constants.HOST);
	}

	@Test
	public void createCounterCF() {
		CommonService comserv = new CommonService(cluster);
		if(null == comserv.getColumnFamily(Constants.KEYSPACE_NAME, cfName)){
			CounterService cs = new CounterService();
			cs.createCounterColumnFamily(cluster, cfName, false);
		}
	}

	@Test
	public void insertCounterCoulumn() {
		CounterService cs = new CounterService();
		Keyspace ks = HFactory.createKeyspace(Constants.KEYSPACE_NAME, cluster);
		cs.insertCounterColumn(ks, cfName, "jayanth", "likes_count", 1l);
	}

	@Test
	public void increementCounterColumn() {
		CounterService cs = new CounterService();
		Keyspace ks = HFactory.createKeyspace(Constants.KEYSPACE_NAME, cluster);
		cs.incrementCounter(ks, cfName, "jayanth", "likes_count");
	}

	@Test
	public void decrementCounterColumn() {
		CounterService cs = new CounterService();
		Keyspace ks = HFactory.createKeyspace(Constants.KEYSPACE_NAME, cluster);
		cs.decrementCounter(ks, cfName, "jayanth", "likes_count");
	}

	@Test
	public void readCounterColumn() {
		CounterService cs = new CounterService();
		Keyspace ks = HFactory.createKeyspace(Constants.KEYSPACE_NAME, cluster);
		HCounterColumn<String> hcc = cs.getCounterColumn(ks, cfName, "jayanth",
				"likes_count");
		System.out.println(hcc.getName() + ":" + hcc.getValue());
	}

	@Test
	public void deleteCounterColumn() {
		CounterService cs = new CounterService();
		Keyspace ks = HFactory.createKeyspace(Constants.KEYSPACE_NAME, cluster);
		cs.deleteCounterColumn(ks, cfName, "jayanth", "likes_count");
	}
	
	@Test
	public void deleteRecord() {
		CounterService cs = new CounterService();
		Keyspace ks = HFactory.createKeyspace(Constants.KEYSPACE_NAME, cluster);
		cs.deleteCounterColumn(ks, cfName, "jayanth", " ");
	}

}
