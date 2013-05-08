package com.strumsoft.cassandra;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.FailoverPolicy;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.factory.HFactory;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.strumsoft.cassandra.constants.Constants;
import com.strumsoft.cassandra.services.CommonService;
import com.strumsoft.cassandra.services.Service;


public class HectorColumnTest {
	private static final String multirowKeyid = "k_insert_info";
	private static final String rowkeyid = "unique_info";
	private static final String cfName = "info";
	private static final String column_domain = "domain";
	private static final String column_company = "company";
	private static final String column_email = "email";
	private static final String column_name = "name";
	private static final Logger log = Logger.getLogger(HectorColumnTest.class);
	private static Cluster clusterObj = null ;
	private static Keyspace keySpace = null ;
	
	@BeforeClass
	public static void initialize() {	
		Map<String, String> credentials = new HashMap<String, String>();
		credentials.put("username", "cassandra"); 
		credentials.put("password", "cassandra");
		
		CassandraHostConfigurator chc = new CassandraHostConfigurator(Constants.HOST);
		//clusterObj = HFactory.getOrCreateCluster(Constants.CLUSTERNAME, chc, credentials);
		clusterObj = HFactory.getOrCreateCluster(Constants.CLUSTERNAME, chc);
		
		ConfigurableConsistencyLevel ccl = new ConfigurableConsistencyLevel();
		Map<String, HConsistencyLevel> clmap = new HashMap<String, HConsistencyLevel>();
		// Define CL.ONE for ColumnFamily "MyColumnFamily"
		clmap.put(cfName, HConsistencyLevel.ONE);
		ccl.setReadCfConsistencyLevels(clmap);
		ccl.setWriteCfConsistencyLevels(clmap);
		
		CommonService cs = new CommonService(clusterObj);
		cs.createKeySpace(Constants.KEYSPACE_NAME);
		keySpace = cs.getKeyspace(Constants.KEYSPACE_NAME, clusterObj, ccl);
	}

	@Test(expected=Exception.class)
	public void createColumnFamily() {
		CommonService cs = new CommonService(clusterObj);
		cs.createColumnFamily(Constants.KEYSPACE_NAME, cfName, false);
	}

	@Test
	public void writeColumn() {
		Service serv = new Service();
		HColumn<String, String> column = HFactory.createColumn(column_name, "strumsoft", Constants.STRING_SERIALIZER, Constants.STRING_SERIALIZER);
		serv.insertColumn(keySpace, rowkeyid, cfName, column );
	}
	@Test
	public void readColumn(){
		Service serv = new Service();
		HColumn<String, String> hc = serv.getColumn(keySpace, cfName, rowkeyid, column_name);
		log.info("Reading.....");
		log.info("++++++++++++++++++++++++++++++");
		log.info(hc.getName()+":"+hc.getValue());
		log.info("++++++++++++++++++++++++++++++");
	}
	@Test
	public void writeColumns() {
		Service serv = new Service();
		List<HColumn<String, String>> columns = new ArrayList<HColumn<String, String>>();
		columns.add(HFactory.createColumn(column_name, "k_rajini_kanth", Constants.STRING_SERIALIZER, Constants.STRING_SERIALIZER));
		columns.add(HFactory.createColumn(column_company, "k_strumsoft", Constants.STRING_SERIALIZER, Constants.STRING_SERIALIZER));
		columns.add(HFactory.createColumn(column_email, "k_rajini_kanth@strumsoft.com", Constants.STRING_SERIALIZER, Constants.STRING_SERIALIZER));
		columns.add(HFactory.createColumn(column_domain, "k_java", Constants.STRING_SERIALIZER, Constants.STRING_SERIALIZER));
		serv.insertColumns(keySpace, multirowKeyid, cfName, columns);
	}
	@Test
	public void readColumns(){
		Service serv = new Service();
		log.info("Getting columns by range");
		List<HColumn<String, String>> hcl = serv.getColumnsByRange(keySpace, cfName, multirowKeyid, "", "");
		log.info("Reading....");
		log.info("++++++++++++++++++++++++++++++");
		for(HColumn<String, String> hc :hcl){
			log.info(hc.getName()+":"+hc.getValue());
		}
		log.info("++++++++++++++++++++++++++++++");
		log.info("Getting columns by names");
		hcl = serv.getColumnsByName(keySpace, cfName, multirowKeyid, column_email);
		log.info("++++++++++++++++++++++++++++++");
		for(HColumn<String, String> hc :hcl){
			log.info(hc.getName()+":"+hc.getValue());
		}
		log.info("++++++++++++++++++++++++++++++");
	}
	
	@Test
	public void getMultigetSlice() {
		Service serv = new Service();
		Rows<String, String, String> rows = serv.getMultiSlice(keySpace, cfName, "", "", "a_insert_info", "multi_insert_info");
		Row<String, String, String> row = rows.getByKey("a_insert_info");
		ColumnSlice<String, String> cs = row.getColumnSlice();
		List<HColumn<String, String>> lhc = cs.getColumns();
			printSubColumn(lhc);
			
		row = rows.getByKey("multi_insert_info");
		cs = row.getColumnSlice();
		lhc = cs.getColumns();
			printSubColumn(lhc);
	}
	
	@Test
	public void getRangeSlice() {
		Service serv = new Service();
		OrderedRows<String, String, String> or = serv.getRangeSlice(keySpace, cfName, "", "", "", "");
		List<Row<String, String, String>>  rows = or.getList();

		for(int i=0; i<rows.size();i++){
			printRow(rows.get(i));
		}		
	}
	
	@Test
	public void deleteColumn() {
		Service serv = new Service();
		serv.deleteColumn(keySpace, rowkeyid, cfName, column_name);
	}
	
	@Test
	public void deleteAllColumns() {
		Service serv = new Service();
		serv.deleteColumn(keySpace, rowkeyid, cfName, null);
	}
	
	@AfterClass
	public static void destroy() {
		CommonService cs = new CommonService(clusterObj);
		cs.shutDownCluster();
	}
	
	private void printRow(Row<String, String, String> row ){
		ColumnSlice<String, String> cs = row.getColumnSlice();
		List<HColumn<String, String>> lhc = cs.getColumns();
			printSubColumn(lhc);
	}
	
	private void printSubColumn(List<HColumn<String, String>> lhsc) {
		log.info("=================================");
		for(HColumn<String,String> hc: lhsc){
			log.info(hc.getName()+":"+hc.getValue());
		}
		log.info("=================================");
	}
}
