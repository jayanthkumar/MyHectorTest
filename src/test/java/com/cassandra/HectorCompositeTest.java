package com.cassandra;


import java.util.ArrayList;
import java.util.List;

import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cassandra.constants.Constants;
import com.cassandra.services.CommonService;
import com.cassandra.services.CompositeKeyService;
/**
 * 
 * @author Shiva
 *
 */
public class HectorCompositeTest {
private static CompositeKeyService compositeKeyService;
private static CommonService commonService;
private static Keyspace keyspace;
private StringSerializer stringSerializer = StringSerializer.get();
private CompositeSerializer compositeSerializer = CompositeSerializer.get();
private DynamicCompositeSerializer dynamicCompositeSerializer = DynamicCompositeSerializer.get();
public static final String COMPOSITE_KEY = "ALL";
public static final String COUNTRY_STATE_CITY_CF = "CountryStateCity";
private static final Logger log =Logger.getLogger(HectorCompositeTest.class);
	@BeforeClass
	public static void oneTimeSetUp() {
		Cluster cluster = HFactory.getOrCreateCluster(Constants.CLUSTERNAME, Constants.HOST);
		keyspace = HFactory.createKeyspace(Constants.KEYSPACE_NAME, cluster);
		compositeKeyService = new CompositeKeyService();
		commonService = new CommonService(cluster);
		log.info("Cluster connection set up created successfully");
	}
	
	@AfterClass
	public static void oneTimeTearDomn() {
		commonService.shutDownCluster();
		log.info("Cluster connection shutdown successfully");
	}
	
	@Test(expected=Exception.class)
	public void testCreateColumnFamily(){
		commonService.createColumnFamily(Constants.KEYSPACE_NAME, COUNTRY_STATE_CITY_CF, false);
	}
	
	@Test
	public void testInsertCompositeKey() {
		List<HColumn<Composite, String>> compositeKeys = getAllComposite();
		compositeKeyService.insertMultipleCompositeKey(keyspace, COMPOSITE_KEY, COUNTRY_STATE_CITY_CF, compositeKeys);
	}
	
	@Test
	public void testReadAllCompositeKeys() {
		String startArg = "US";
		// Note the use of 'equal' and 'greater-than-equal' for the start and end.
	    // this has to be the case when we want all 
	    Composite start = compositeFrom(startArg, Composite.ComponentEquality.EQUAL);
	    Composite end = compositeFrom(startArg, Composite.ComponentEquality.GREATER_THAN_EQUAL);
	    List<HColumn<Composite, String>> lhc = compositeKeyService.readAllCompositeKeyByRange(keyspace, COUNTRY_STATE_CITY_CF, COMPOSITE_KEY, start, end);
	    printCompositeColumn(lhc);
	}
	
	@Test
	public void testInsertDynamicCompositeKey() {
		List<HColumn<DynamicComposite, String>> dynamicCompositeKeys = getAllDynamicComposite();
		compositeKeyService.insertMultipleDynamicCompositeKey(keyspace, COMPOSITE_KEY, COUNTRY_STATE_CITY_CF, dynamicCompositeKeys);
	}
	
	@Test
	public void testReadAllDynamicCompositeKeys() {
		String startArg = "US";
		// Note the use of 'equal' and 'greater-than-equal' for the start and end.
	    // this has to be the case when we want all 
	    DynamicComposite start = dynamicCompositeFrom(startArg, DynamicComposite.ComponentEquality.EQUAL);
	    DynamicComposite end = dynamicCompositeFrom(startArg, DynamicComposite.ComponentEquality.GREATER_THAN_EQUAL);
	    List<HColumn<DynamicComposite, String>> lhc = compositeKeyService.readAllDynamicCompositeKeyByRange(keyspace, COUNTRY_STATE_CITY_CF, COMPOSITE_KEY, start, end);
	    printDynamicCompositeColumn(lhc);
	}
	
	@Test
	public void testUpdateCompositeKey() {
		String startArg = "US";
		// Note the use of 'equal' and 'greater-than-equal' for the start and end.
	    // this has to be the case when we want all 
	    Composite start = compositeFrom(startArg, Composite.ComponentEquality.EQUAL);
	    Composite end = compositeFrom(startArg, Composite.ComponentEquality.GREATER_THAN_EQUAL);
		List<HColumn<Composite, String>> cks = compositeKeyService.readAllCompositeKeyByRange(keyspace, COUNTRY_STATE_CITY_CF, COMPOSITE_KEY, start, end);
		Composite composite = new Composite();
		composite.addComponent("US", StringSerializer.get());
		composite.addComponent("WI", StringSerializer.get());
		composite.addComponent("Oshkosh_update", StringSerializer.get());
		HColumn<Composite, String> col = HFactory
				.createColumn(composite, "America/Chicago_update",
						compositeSerializer, stringSerializer);
		cks.add(col);
		compositeKeyService.updateMutliCompositeKeys(keyspace, COMPOSITE_KEY, COUNTRY_STATE_CITY_CF, cks);
	}
	
	@Test
	public void testDeleteCompositeKeys() {
		Composite composite = new Composite();
		composite.addComponent("US", StringSerializer.get());
		composite.addComponent("WI", StringSerializer.get());
		composite.addComponent("Oshkosh", StringSerializer.get());
		compositeKeyService.deleteAllCompositeKeys(keyspace, COMPOSITE_KEY, COUNTRY_STATE_CITY_CF, composite);
		String startArg = "US";
		// Note the use of 'equal' and 'greater-than-equal' for the start and end.
	    // this has to be the case when we want all 
	    Composite start = compositeFrom(startArg, Composite.ComponentEquality.EQUAL);
	    Composite end = compositeFrom(startArg, Composite.ComponentEquality.GREATER_THAN_EQUAL);
		List<HColumn<Composite, String>> cks = compositeKeyService.readAllCompositeKeyByRange(keyspace, COUNTRY_STATE_CITY_CF, COMPOSITE_KEY, start, end);
		log.info("Deleted all composite keys sucessfully "+cks);
		printCompositeColumn(cks);
	}
	
	@Test
	public void testDeleteAllCompositeKeys() {
		compositeKeyService.deleteAllCompositeKeys(keyspace, COMPOSITE_KEY, COUNTRY_STATE_CITY_CF, null);
	}
	
	 /**
	   *  Get composite object
	   * 
	   * @param componentName
	   * @param equalityOp
	   * @return
	   */
	  private Composite compositeFrom(String componentName, Composite.ComponentEquality equalityOp) {
	    Composite composite = new Composite();
	    composite.addComponent(0, componentName, equalityOp);
	    return composite;
	  }
	  
	  /**
	   *  Get dynamic composite object
	   *  
	   * @param componentName
	   * @param equalityOp
	   * @return
	   */
	private DynamicComposite dynamicCompositeFrom(String componentName,
			DynamicComposite.ComponentEquality equalityOp) {
		DynamicComposite composite = new DynamicComposite();
		composite.addComponent(0, componentName, equalityOp);
		return composite;
	}

	/**
	 * Get all composite keys
	 * 
	 * @return
	 */
	private List<HColumn<Composite, String>> getAllComposite() {
		List<HColumn<Composite, String>> compositeColumns = new ArrayList<HColumn<Composite, String>>();
		Composite composite = new Composite();
		composite.addComponent("US", StringSerializer.get());
		composite.addComponent("WI", StringSerializer.get());
		composite.addComponent("Oshkosh", StringSerializer.get());
		HColumn<Composite, String> col = HFactory.createColumn(composite,
				"America/Chicago", compositeSerializer, stringSerializer);
		compositeColumns.add(col);

		Composite composite_one = new Composite();
		composite_one.addComponent("US", StringSerializer.get());
		composite_one.addComponent("WI", StringSerializer.get());
		composite_one.addComponent("Sheboygan", StringSerializer.get());
		HColumn<Composite, String> column = HFactory.createColumn(
				composite_one, "America/Chicago", compositeSerializer,
				stringSerializer);
		compositeColumns.add(column);
		return compositeColumns;
	}
	
	/**
	 * Get all dynamic composite keys
	 * 
	 * @return
	 */
	private List<HColumn<DynamicComposite, String>> getAllDynamicComposite() {
		List<HColumn<DynamicComposite, String>> dynamicCompositeColumns = new ArrayList<HColumn<DynamicComposite,String>>();
		DynamicComposite dynamicComposite = new DynamicComposite();
		dynamicComposite.addComponent("US", StringSerializer.get());
		dynamicComposite.addComponent("WI", StringSerializer.get());
		dynamicComposite.addComponent("Oshkosh1", StringSerializer.get());
		HColumn<DynamicComposite, String> col = HFactory.createColumn(dynamicComposite, "America/Chicago",dynamicCompositeSerializer,
				stringSerializer);
		dynamicCompositeColumns.add(col);
		
		DynamicComposite dynamicComposite_one = new DynamicComposite();
		dynamicComposite_one.addComponent("US", StringSerializer.get());
		dynamicComposite_one.addComponent("WI", StringSerializer.get());
		dynamicComposite_one.addComponent("Sheboygan1", StringSerializer.get());
		HColumn<DynamicComposite, String> column = HFactory.createColumn(dynamicComposite_one, "America/Chicago",dynamicCompositeSerializer,
				stringSerializer);
		dynamicCompositeColumns.add(column);
		return dynamicCompositeColumns;
	}
	
	private void printCompositeColumn(List<HColumn<Composite, String>> lhsc) {
		log.info("=================================");
		for(HColumn<Composite,String> hc: lhsc){
			log.info(hc.getName()+":"+hc.getValue());
		}
		log.info("=================================");
	}
	private void printDynamicCompositeColumn(List<HColumn<DynamicComposite, String>> lhsc) {
		log.info("=================================");
		for(HColumn<DynamicComposite,String> hc: lhsc){
			log.info(hc.getName()+":"+hc.getValue());
		}
		log.info("=================================");
	}
}
