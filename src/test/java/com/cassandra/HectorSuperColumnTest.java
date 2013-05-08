package com.cassandra;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import me.prettyprint.cassandra.model.AllOneConsistencyLevelPolicy;
import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.FailoverPolicy;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.OrderedSuperRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.beans.SuperRow;
import me.prettyprint.hector.api.beans.SuperRows;
import me.prettyprint.hector.api.factory.HFactory;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cassandra.constants.Constants;
import com.cassandra.services.CommonService;
import com.cassandra.services.SuperService;
/**
 * 
 * @author Jayanth Kumar
 *
 */
public class HectorSuperColumnTest {
	private static final String cfName = "Contacts";
	private static final String rowkeyid_a = "a_info";
	private static final String rowkeyid_m = "m_info";
	private static final String rowkeyid_z = "z_info";
	private static final String email_super_name = "emails";
	private static final String likes_super_name = "likes";
	private static final String following_super_name = "following";
	private static final String followers_super_name = "followers";
	private static final Logger log = Logger.getLogger(HectorSuperColumnTest.class);
	private static Cluster clusterObj = null ;
	private static Keyspace keySpace = null ;
	
@BeforeClass
public static void initialize() {
	// authentication
	Map<String, String> credentials = new HashMap<String, String>();
	credentials.put("username", "cassandra"); 
	credentials.put("password", "cassandra");
	
	CassandraHostConfigurator chc = new CassandraHostConfigurator(Constants.HOST);
	clusterObj = HFactory.getOrCreateCluster(Constants.CLUSTERNAME, chc, credentials);
	//clusterObj = HFactory.getOrCreateCluster(Constants.CLUSTERNAME, chc);
	
	// To configure variable consistency
	ConfigurableConsistencyLevel ccl = new ConfigurableConsistencyLevel();
	Map<String, HConsistencyLevel> clmap = new HashMap<String, HConsistencyLevel>();
	// Define CL.ONE for ColumnFamily "MyColumnFamily"
	clmap.put(cfName, HConsistencyLevel.ONE);
	ccl.setReadCfConsistencyLevels(clmap);
	ccl.setWriteCfConsistencyLevels(clmap);
	
	keySpace = HFactory.createKeyspace(Constants.KEYSPACE_NAME, clusterObj, ccl, FailoverPolicy.ON_FAIL_TRY_ALL_AVAILABLE, credentials);
}
@Test
public void createKeySpace(){
	CommonService cs = new CommonService(clusterObj);
	assertNotNull(cs.createKeySpace(Constants.KEYSPACE_NAME));
}
@Test(expected = Exception.class)
public void createColumnFamily(){
	CommonService cs = new CommonService(clusterObj);
	cs.createColumnFamily(Constants.KEYSPACE_NAME, cfName, true);
}
@Test
public void insertAndGetSuperColumn(){
	SuperService sc = new SuperService();
	
	// Insert Multiple super columns emails,likepages,followers,following
	Map<String,String> emailMap = new HashMap<String, String>();
	emailMap.put("email1", "strum_a@gmail.com");
	emailMap.put("email2", "strumsoft_a@strumsoft.com");
	emailMap.put("skype", "strumsoftltd_a");
	
	
	Map<String,String> likesMap = new HashMap<String, String>();
	likesMap.put("Actors","Aish_a");
	likesMap.put("company","Strumsoft_a");
	likesMap.put("Domain","java_a");
	
	Set<String> followersSet = new HashSet<String>();
	followersSet.add("verizon_a");
	followersSet.add("nfl_a");
	followersSet.add("intertrust_a");
	
	Set<String> followingSet = new HashSet<String>();
	followingSet.add("yob_a");
	followingSet.add("acra_a");
	followingSet.add("ambient_a");
	
	// insert super column
	
	sc.insertSuperColumn(keySpace, rowkeyid_a, cfName, sc.getSuperColumnObjFromMap(emailMap, email_super_name));
	sc.insertSuperColumn(keySpace, rowkeyid_a, cfName, sc.getSuperColumnObjFromMap(likesMap, likes_super_name));
	sc.insertSuperColumn(keySpace, rowkeyid_a, cfName, sc.getSuperColumnObjFromSet(followersSet, following_super_name));
	sc.insertSuperColumn(keySpace, rowkeyid_a, cfName, sc.getSuperColumnObjFromSet(followersSet, followers_super_name));
	
	// read whether inserted or not
	HSuperColumn<String, String, String> readSuperColumn = sc.getSuperColumn(keySpace, cfName, rowkeyid_a, email_super_name);
	assertNotNull(readSuperColumn);
	printSuperColumn(readSuperColumn);
	readSuperColumn = sc.getSuperColumn(keySpace, cfName, rowkeyid_a, likes_super_name);
	assertNotNull(readSuperColumn);
	printSuperColumn(readSuperColumn);
	readSuperColumn = sc.getSuperColumn(keySpace, cfName, rowkeyid_a, following_super_name);
	assertNotNull(readSuperColumn);
	printSuperColumn(readSuperColumn);
	readSuperColumn = sc.getSuperColumn(keySpace, cfName, rowkeyid_a, followers_super_name);
	assertNotNull(readSuperColumn);
	printSuperColumn(readSuperColumn);
}

// To insert for multiple rows
@Test
public void insertAndGetSuperColumn_m(){
	SuperService sc = new SuperService();
	
	// Insert Multiple super columns emails,likepages,followers,following
	Map<String,String> emailMap = new HashMap<String, String>();
	emailMap.put("email1", "strum_m@gmail.com");
	emailMap.put("email2", "strumsoft_m@strumsoft.com");
	emailMap.put("skype", "strumsoftltd_m");
	
	
	Map<String,String> likesMap = new HashMap<String, String>();
	likesMap.put("Actors","Aish_m");
	likesMap.put("company","Strumsoft_m");
	likesMap.put("Domain","java_m");
	
	Set<String> followersSet = new HashSet<String>();
	followersSet.add("verizon_m");
	followersSet.add("nfl_m");
	followersSet.add("intertrust_m");
	
	Set<String> followingSet = new HashSet<String>();
	followingSet.add("yob_m");
	followingSet.add("acra_m");
	followingSet.add("ambient_m");
	
	// insert super column
	
	sc.insertSuperColumn(keySpace, rowkeyid_m, cfName, sc.getSuperColumnObjFromMap(emailMap, email_super_name));
	sc.insertSuperColumn(keySpace, rowkeyid_m, cfName, sc.getSuperColumnObjFromMap(likesMap, likes_super_name));
	sc.insertSuperColumn(keySpace, rowkeyid_m, cfName, sc.getSuperColumnObjFromSet(followersSet, following_super_name));
	sc.insertSuperColumn(keySpace, rowkeyid_m, cfName, sc.getSuperColumnObjFromSet(followersSet, followers_super_name));
}

@Test
public void insertAndGetSuperColumn_z(){
	SuperService sc = new SuperService();
	
	// Insert Multiple super columns emails,likepages,followers,following
	Map<String,String> emailMap = new HashMap<String, String>();
	emailMap.put("email1", "strum_z@gmail.com");
	emailMap.put("email2", "strumsoft_z@strumsoft.com");
	emailMap.put("skype", "strumsoftltd_z");
	
	
	Map<String,String> likesMap = new HashMap<String, String>();
	likesMap.put("Actors","Aish_z");
	likesMap.put("company","Strumsoft_z");
	likesMap.put("Domain","java_z");
	
	Set<String> followersSet = new HashSet<String>();
	followersSet.add("verizon_z");
	followersSet.add("nfl_z");
	followersSet.add("intertrust_z");
	
	Set<String> followingSet = new HashSet<String>();
	followingSet.add("yob_z");
	followingSet.add("acra_z");
	followingSet.add("ambient_z");
	
	// insert super column
	
	sc.insertSuperColumn(keySpace, rowkeyid_z, cfName, sc.getSuperColumnObjFromMap(emailMap, email_super_name));
	sc.insertSuperColumn(keySpace, rowkeyid_z, cfName, sc.getSuperColumnObjFromMap(likesMap, likes_super_name));
	sc.insertSuperColumn(keySpace, rowkeyid_z, cfName, sc.getSuperColumnObjFromSet(followersSet, following_super_name));
	sc.insertSuperColumn(keySpace, rowkeyid_z, cfName, sc.getSuperColumnObjFromSet(followersSet, followers_super_name));
}

@Test
public void insertAndGetSuperColumnUpdate(){
	String SUPER_COLUMN_NAME = "emails";
	Map<String,String> map = new HashMap<String, String>();
	map.put("email1", "strum_update_a@gmail.com");
	map.put("email2", "strumsoft_update_a@strumsoft.com");
	// insert super column ie update existing 
	SuperService sc = new SuperService();
	sc.insertSuperColumn(keySpace, rowkeyid_a, cfName, sc.getSuperColumnObjFromMap(map, SUPER_COLUMN_NAME));
		
	HSuperColumn<String, String, String> readSuperColumn = sc.getSuperColumn(keySpace, cfName, rowkeyid_a, SUPER_COLUMN_NAME);
	assertNotNull(readSuperColumn);
	printSuperColumn(readSuperColumn);
}

@Test
public void getSuperSlice(){
	SuperService sc = new SuperService();
	// getting slice for likes and following super slice by column names for rowkeyid_a
	log.info("Getting columns by name");
	List<HSuperColumn<String, String, String>> supcol = sc.getSuperSliceByNames(keySpace, cfName, rowkeyid_a, likes_super_name, following_super_name);
	for(HSuperColumn<String, String, String> hsc : supcol){
		printSuperColumn(hsc);
	}
	log.info("Getting columns by range");
	supcol = sc.getSuperSliceByRange(keySpace, cfName, rowkeyid_a, "", "k");
	for(HSuperColumn<String, String, String> hsc : supcol){
		printSuperColumn(hsc);
	}
}

@Test
public void getSubSlice(){
	SuperService sc = new SuperService();
	// getting slice of subcolumns of a particular super column by name
	log.info("Getting subcolumns by name");
	List<HColumn<String, String>> hcol = sc.getSubSliceByNames(keySpace, cfName, rowkeyid_a, email_super_name, "email1", "email2");
	printSubColumn(hcol);
	log.info("Getting subcolumns by range");
	// getting slice of subcolumns of a particular super column by range
	hcol = sc.getSubSliceByRange(keySpace, cfName, rowkeyid_a, email_super_name, "f","");
	assertNotNull(hcol);
	printSubColumn(hcol);
}

@Test
public void getMultigetSuperSlice(){
	SuperService sc = new SuperService();
	Set<String> columnNames = new HashSet<String>();
	columnNames.add("email1");
	columnNames.add("skype");
	Set<String> keys = new HashSet<String>();
	keys.add(rowkeyid_a);
	keys.add(rowkeyid_z);
	SuperRows<String, String, String, String> sr = sc.getMultigetSuperSliceQuery(keySpace, cfName, columnNames, keys, "a", "f");
	
	SuperRow<String, String, String, String> srro =sr.getByKey(rowkeyid_a);
	printSuperRow(srro);
	
	srro =sr.getByKey(rowkeyid_z);
	printSuperRow(srro);
}

@Test
public void getMultigetSubSlice() {
	SuperService sc = new SuperService();
	Set<String> keys = new HashSet<String>();
	keys.add(rowkeyid_a);
	keys.add(rowkeyid_z);
	Set<String> columnNames = new HashSet<String>();
	// getting only email1 column from all particular records
	columnNames.add("email1");
	Rows<String, String, String> rows = sc.getMultigetSubSliceQuery(keySpace, cfName, keys, email_super_name, columnNames);
	
	Row<String, String, String> row = rows.getByKey(rowkeyid_a);
	printRow(row);
	
	row = rows.getByKey(rowkeyid_z);
	printRow(row);
}

@Test
public void getRangeSuperSlice() {
	SuperService sc = new SuperService();
	OrderedSuperRows<String, String, String, String> osr = sc.getRangeSuperSliceQuery(keySpace, cfName, "", "", "", "");
	List<SuperRow<String, String, String, String>>  sr = osr.getList();
	for(SuperRow<String, String, String, String> sro : sr){
		printSuperRow(sro);
	}
}

@Test
public void getRangeSubSlice() {
	SuperService sc = new SuperService();
	OrderedRows<String, String, String> or = sc.getRangeSubSliceQuery(keySpace, cfName, email_super_name, "", "", "", "");
	List<Row<String, String, String>> lro = or.getList();
	for(Row<String, String, String> row: lro){
		printRow(row);
	}
}

@Test
public void deleteSubColumn(){
	String SUPER_COLUMN_NAME = email_super_name;
	SuperService sc = new SuperService();
	String sbName = "email1";
	sc.deleteSubColumn(keySpace, rowkeyid_a, cfName, SUPER_COLUMN_NAME, sbName);
	
	printSuperColumn(sc.getSuperColumn(keySpace, cfName, rowkeyid_a, SUPER_COLUMN_NAME));
	
	// read whether deleted or not
	HColumn<String,String> hco = sc.readSubColumn(keySpace, cfName, rowkeyid_a, SUPER_COLUMN_NAME, sbName);
	assertNull(hco);
}

@Test
public void deleteSuperColumn(){
	String SUPER_COLUMN_NAME = email_super_name;
	SuperService sc = new SuperService();
	sc.deleteSuperColumn(keySpace, rowkeyid_a, cfName, SUPER_COLUMN_NAME);
	
	// read whether inserted or not
	HSuperColumn<String, String, String>  hsc = sc.getSuperColumn(keySpace, cfName, rowkeyid_a, SUPER_COLUMN_NAME);
	assertNull(hsc);
}

@Test
public void deleteRecord(){
	CommonService cs = new CommonService(HFactory.getOrCreateCluster(Constants.CLUSTERNAME, Constants.HOST));
	cs.deleteRecord(rowkeyid_a, cfName);
}

@AfterClass
public static void destroy() {
	CommonService cs = new CommonService(clusterObj);
	cs.shutDownCluster();
}

private void printSuperColumn(HSuperColumn<String, String, String> lhsc) {
	log.info("Columns for : "+lhsc.getName());
	List<HColumn<String, String>>  lhc = lhsc.getColumns();
	printSubColumn(lhc);
}

private void printSubColumn(List<HColumn<String, String>> lhsc) {
	log.info("=================================");
	for(HColumn<String,String> hc: lhsc){
		log.info(hc.getName()+":"+hc.getValue());
	}
	log.info("=================================");
}

private void printSuperRow(SuperRow<String, String, String, String> sr) {
	List<HSuperColumn<String,String,String>> scList = sr.getSuperSlice().getSuperColumns();
	log.info("===============*==================");
	for(HSuperColumn<String,String,String> hsc : scList){
		printSuperColumn(hsc);
	}
	log.info("===============*==================");
}

private void printRow(Row<String, String, String> row) {
	List<HColumn<String, String>> lhc = row.getColumnSlice().getColumns();
	printSubColumn(lhc);
}
}
