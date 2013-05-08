package com.cassandra;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.yaml.snakeyaml.scanner.Constant;

import static org.junit.Assert.*;

import com.cassandra.constants.Constants;
import com.cassandra.objects.Employee;
import com.cassandra.services.CommonService;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.ddl.ColumnIndexType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hom.EntityManagerImpl;

/**
 * 
 * @author Jayanth Kumar
 *
 */
public class HomTest {
	private static final Logger log = Logger.getLogger(HomTest.class);
	private static Cluster cluster = null;
	private static Keyspace keyspace = null;
	private static EntityManagerImpl em = null;
	
	@BeforeClass
	public static void initialize() {
		cluster = HFactory.getOrCreateCluster(Constants.CLUSTERNAME, Constants.HOST);
		keyspace = HFactory.createKeyspace(Constants.KEYSPACE_NAME, cluster);
		CommonService cs = new CommonService(cluster);
		if(null == cs.getColumnFamily(Constants.KEYSPACE_NAME, "employee")){
			cs.createColumnFamily(Constants.KEYSPACE_NAME, "employee", false);
		}
		em = new EntityManagerImpl(keyspace, "com.cassandra");
	}

	@Test
	public void insertObject() {
		Employee emp1 = new Employee();
		emp1.setEmpId(1l);
		emp1.setName("tester");
		emp1.setMailId("tester@gmail.com");
		emp1.setGender("male");
		Calendar cal1 = new GregorianCalendar(1990, 00, 01);
		emp1.setDob(cal1.getTime());
		emp1.setSalary(15l);
		List<String> projectGroup = new ArrayList<String>();
		projectGroup.add("Intertrust");
		projectGroup.add("voiby");
		emp1.setProjectGroup(projectGroup );
		Map<String, String> projectInfoMap = new HashMap<String, String>();
		projectInfoMap.put("started", "no");
		projectInfoMap.put("progress", "yes");
		emp1.setProjectInfo(projectInfoMap);
		
		// 2nd record
		Employee emp2 = new Employee();
		emp2.setEmpId(2l);
		emp2.setName("tester2");
		emp2.setMailId("tester2@gmail.com");
		emp2.setGender("male");
		Calendar cal2 = new GregorianCalendar(1992, 00, 01);
		emp2.setDob(cal2.getTime());
		emp2.setSalary(20l);
		List<String> projectGroup2 = new ArrayList<String>();
		projectGroup2.add("Intertrust");
		projectGroup2.add("voiby");
		emp2.setProjectGroup(projectGroup );
		Map<String, String> projectInfoMap2 = new HashMap<String, String>();
		projectInfoMap2.put("started", "no");
		projectInfoMap2.put("progress", "yes");
		emp2.setProjectInfo(projectInfoMap2);
		
		// 3rd record
		
		
		em.persist(emp1);
		em.persist(emp2);
		log.info("persisted");
	}
	
	@Test
	public void readObject() {
		Employee pojo2 = em.find(Employee.class, 1l);
		assertNotNull(pojo2);
		log.info("ResultSet :" + pojo2);
	}
	
	@Test
	public void getRangeSlice() {
		ColumnSlice<String, byte[]> cs;
		RangeSlicesQuery<Long, String, Long> rsq = HFactory.createRangeSlicesQuery(keyspace, LongSerializer.get(), StringSerializer.get(), LongSerializer.get());
		rsq.setColumnFamily("employee");
		rsq.setRange("", "", false, 100);
		rsq.setRowCount(100);
		rsq.addGtExpression("salary", 12l);
		QueryResult<OrderedRows<Long, String, Long>>  qr = rsq.execute();		
		OrderedRows<Long, String, Long> or = qr.get();
		//log.info("peek :"+or.peekLast());
		List<Row<Long, String, Long>> lro = or.getList(); log.info("size"+lro.size());
		for(Row<Long, String, Long> row: lro){
			log.info("key "+row.getKey()+" :"+em.find(Employee.class, row.getKey()));
		}
	}
	
	@Test
	public void deleteRow() {
		Long id = 2l;
		Mutator<Long> mutator = HFactory.createMutator(keyspace, LongSerializer.get());
		mutator.addDeletion(id, "employee");
		mutator.execute();
		log.info("Row Deleted :"+id);
	}
	
	@Test
	public void updateObject() {
		// updating
		Employee pojo2 = em.find(Employee.class, 1l);
		pojo2.setName("tester_modified");
		pojo2.setMailId("tester_modified@gmail.com");
		em.persist(pojo2);
		pojo2 = em.find(Employee.class, 1l);
		assertNotNull(pojo2);
		log.info("Updated ResultSet :" + pojo2);
	}
	
	@AfterClass
	public static void destroy() {
		CommonService cs = new CommonService(cluster);
		cs.shutDownCluster();
	}
}
