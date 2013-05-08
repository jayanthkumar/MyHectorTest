package com.cassandra.services;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.cassandra.constants.Constants;

import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.OrderedSuperRows;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.beans.SuperRows;
import me.prettyprint.hector.api.beans.SuperSlice;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.MultigetSubSliceQuery;
import me.prettyprint.hector.api.query.MultigetSuperSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSubSlicesQuery;
import me.prettyprint.hector.api.query.RangeSuperSlicesQuery;
import me.prettyprint.hector.api.query.SubColumnQuery;
import me.prettyprint.hector.api.query.SubSliceQuery;
import me.prettyprint.hector.api.query.SuperColumnQuery;
import me.prettyprint.hector.api.query.SuperSliceQuery;

public class SuperService {
	private static final Logger log = Logger.getLogger(SuperService.class);
	
	/**
	 * Inserts a SuperColumn
	 * @param keySpace The Keyspace object 
	 * @param rowkeyid The row key of a record to which super column belongs
	 * @param cfName The column Family name
	 * @param superColumn HSuperColumn Object
	 */
	public void insertSuperColumn(Keyspace keySpace, String rowkeyid,
			String cfName, HSuperColumn<String, String, String> superColumn) {
		Mutator<String> mutator = HFactory.createMutator(keySpace, Constants.STRING_SERIALIZER);
		mutator.addInsertion(rowkeyid, cfName, superColumn);
		MutationResult mutationResult = mutator.execute();
		log.info("Insert Super Column " + superColumn.getName() + ": " + mutationResult.toString());
	}
	
	/**
	 * To get a particular super column 
	 * @param keySpace The Keyspace object 
	 * @param cfName The column family name 
	 * @param rowkeyid The row key of a record 
	 * @param SUPER_COLUMN_NAME The super column name
	 * @return Returns HSuperColumn object
	 */
	public HSuperColumn<String, String, String> getSuperColumn(
			Keyspace keySpace, String cfName, String rowkeyid,
			String SUPER_COLUMN_NAME) {
		SuperColumnQuery<String, String, String, String> scq = HFactory
				.createSuperColumnQuery(keySpace, Constants.STRING_SERIALIZER,
						Constants.STRING_SERIALIZER,
						Constants.STRING_SERIALIZER,
						Constants.STRING_SERIALIZER);
		scq.setColumnFamily(cfName);
		scq.setSuperName(SUPER_COLUMN_NAME);
		scq.setKey(rowkeyid);
		QueryResult<HSuperColumn<String, String, String>> qr = scq.execute();
		HSuperColumn<String, String, String> hsc = qr.get();
		//log.info("Super Column " + hsc.getName() + " reading.....");
		return hsc;
	}
	
	/**
	 * To get a particular sub column
	 * 
	 * @param keySpace The Keyspace object 
	 * @param cfName The column family name 
	 * @param rowkeyid The row key of a record 
	 * @param SUPER_COLUMN_NAME The super column name
	 * @param SUB_COLUMN_NAME The sub column name
	 * @return Returns HColumn object
	 */
	public HColumn<String, String> readSubColumn(Keyspace keySpace,
			String cfName, String rowkeyid, String SUPER_COLUMN_NAME,
			String SUB_COLUMN_NAME) {
		SubColumnQuery<String, String, String, String> sucq = HFactory
				.createSubColumnQuery(keySpace, Constants.STRING_SERIALIZER,
						Constants.STRING_SERIALIZER,
						Constants.STRING_SERIALIZER,
						Constants.STRING_SERIALIZER);
		sucq.setColumnFamily(cfName);
		sucq.setKey(rowkeyid);
		sucq.setSuperColumn(SUPER_COLUMN_NAME);
		sucq.setColumn(SUB_COLUMN_NAME);

		QueryResult<HColumn<String, String>> qr = sucq.execute();
		HColumn<String, String> hc = qr.get();
		//log.info("Super SubColumn " + hc.getName() + " reading.....");
		return hc;
	}
	
	/**
	 * To get slice of Super Columns
	 * @param keySpace The Keyspace object 
	 * @param cfName The column family name
	 * @param rowkeyid The row key of a record 
	 * @param columnNames The names of the super column to fetch
	 * @return Returns a list of HsuperColumns for the specified row
	 */
	public List<HSuperColumn<String, String, String>> getSuperSliceByNames(
			Keyspace keySpace, String cfName, String rowkeyid, String... columnNames) {
		SuperSliceQuery<String, String, String, String> ssql = HFactory
				.createSuperSliceQuery(keySpace, Constants.STRING_SERIALIZER,
						Constants.STRING_SERIALIZER,
						Constants.STRING_SERIALIZER,
						Constants.STRING_SERIALIZER);
		ssql.setColumnFamily(cfName);
		ssql.setKey(rowkeyid);
		ssql.setColumnNames(columnNames);

		QueryResult<SuperSlice<String, String, String>> supsl = ssql.execute();
		SuperSlice<String, String, String> sups = supsl.get();
		return sups.getSuperColumns();
	}

	/**
	 * To get slice of Super Columns
	 * @param keySpace The Keyspace object
	 * @param cfName The column family name
	 * @param rowkeyid The row key of a record
	 * @param start The start range for super column
	 * @param end The end range for super column
	 * @return Returns a list of HsuperColumns for the specified row
	 */
	public List<HSuperColumn<String, String, String>> getSuperSliceByRange(
			Keyspace keySpace, String cfName, String rowkeyid, String start, String end) {
		SuperSliceQuery<String, String, String, String> ssql = HFactory
				.createSuperSliceQuery(keySpace, Constants.STRING_SERIALIZER,
						Constants.STRING_SERIALIZER,
						Constants.STRING_SERIALIZER,
						Constants.STRING_SERIALIZER);
		ssql.setColumnFamily(cfName);
		ssql.setKey(rowkeyid);
		ssql.setRange(start, end, false, 100);

		QueryResult<SuperSlice<String, String, String>> supsl = ssql.execute();
		SuperSlice<String, String, String> sups = supsl.get();
		return sups.getSuperColumns();
	}
	
	/**
	 * To get slice of Sub Columns
	 * @param keySpace The Keyspace object
	 * @param cfName The column family name
	 * @param rowkeyid The row key of a record
	 * @param superColumn The name of the super column
	 * @param columnNames Column names to be fetched
	 * @return
	 */
	public List<HColumn<String, String>> getSubSliceByNames(Keyspace keySpace,
			String cfName, String rowkeyid, String superColumn, String... columnNames) {
		SubSliceQuery<String, String, String, String> ssq = HFactory
				.createSubSliceQuery(keySpace, Constants.STRING_SERIALIZER,
						Constants.STRING_SERIALIZER,
						Constants.STRING_SERIALIZER,
						Constants.STRING_SERIALIZER);
		ssq.setColumnFamily(cfName);
		ssq.setKey(rowkeyid);
		ssq.setSuperColumn(superColumn);
		ssq.setColumnNames(columnNames);
		QueryResult<ColumnSlice<String, String>> qr = ssq.execute();
		return qr.get().getColumns();
	}
	
	/**
	 * To get subslice of a super column 
	 * @param keySpace The Keyspace object
	 * @param cfName The column family name
	 * @param rowkeyid The row key of a record
	 * @param superColumn The name of the super column
	 * @param start The starting range of columns
	 * @param end The ending range of columns
	 * @return Returns a list of column in the specified range
	 */
	public List<HColumn<String, String>> getSubSliceByRange(Keyspace keySpace,
			String cfName, String rowkeyid, String superColumn, String start, String end) {
		SubSliceQuery<String, String, String, String> ssq = HFactory
				.createSubSliceQuery(keySpace, Constants.STRING_SERIALIZER,
						Constants.STRING_SERIALIZER,
						Constants.STRING_SERIALIZER,
						Constants.STRING_SERIALIZER);
		ssq.setColumnFamily(cfName);
		ssq.setKey(rowkeyid);
		ssq.setSuperColumn(superColumn);
		ssq.setRange(start, end, false, 5);
		QueryResult<ColumnSlice<String, String>> qr = ssq.execute();
		return qr.get().getColumns();
	}
	
	/**
	 * To get Multiple records/rows for a particular set of rowkeys which contain specified super columns
	 * @param keyspace The Keyspace object
	 * @param cfName The column family name
	 * @param columnNames Collection of super column names to fetch
	 * @param keys Collection of unique row keys
	 * @param start The starting range of columns
	 * @param finish The ending range of columns
	 * @return Returns SuperRows 
	 */
	public SuperRows<String, String, String, String> getMultigetSuperSliceQuery(
			Keyspace keyspace, String cfName, Collection<String> columnNames,
			Collection<String> keys, String start, String finish) {
		MultigetSuperSliceQuery<String, String, String, String> mssq = HFactory
				.createMultigetSuperSliceQuery(keyspace,
						Constants.STRING_SERIALIZER,
						Constants.STRING_SERIALIZER,
						Constants.STRING_SERIALIZER,
						Constants.STRING_SERIALIZER);
		mssq.setColumnFamily(cfName);
		mssq.setColumnNames(columnNames);
		mssq.setKeys(keys);
		mssq.setRange(start, finish, false, 5);
		QueryResult<SuperRows<String, String, String, String>> qr = mssq
				.execute();
		return qr.get();
	}
	
	/**
	 * To get slice of sub columns in a super column for a particular set of rowkeys
	 * @param keyspace The Keyspace object
	 * @param cfName The column family name
	 * @param keys Collection of unique row keys
	 * @param superColumnName The super column name
	 * @param columnNames Sub Column names to be fetched
	 * @return Returns Rows Object
	 */
	public Rows<String, String, String> getMultigetSubSliceQuery(
			Keyspace keyspace, String cfName, Collection<String> keys,
			String superColumnName, Collection<String> columnNames) {
		MultigetSubSliceQuery<String, String, String, String> mssq = HFactory
				.createMultigetSubSliceQuery(keyspace,
						Constants.STRING_SERIALIZER,
						Constants.STRING_SERIALIZER,
						Constants.STRING_SERIALIZER,
						Constants.STRING_SERIALIZER);
		mssq.setColumnFamily(cfName);
		mssq.setKeys(keys);
		mssq.setSuperColumn(superColumnName);
		mssq.setColumnNames(columnNames);
		QueryResult<Rows<String, String, String>> qr = mssq.execute();
		return qr.get();
	}
	
	/**
	 * To get range of records/rows 
	 * @param keyspace The Keyspace object
	 * @param cfName The column family name
	 * @param keystart The start key value
	 * @param keyend The end key value
	 * @param rangestart The start range of columns
	 * @param rangefinish The end range of columns
	 * @return Returns OrderedSuperRows which contains a set of rows
	 */ 
	public OrderedSuperRows<String, String, String, String> getRangeSuperSliceQuery(
			Keyspace keyspace, String cfName, String keystart, String keyend,
			String rangestart, String rangefinish) {
		RangeSuperSlicesQuery<String, String, String, String> rssq = HFactory
				.createRangeSuperSlicesQuery(keyspace,
						Constants.STRING_SERIALIZER,
						Constants.STRING_SERIALIZER,
						Constants.STRING_SERIALIZER,
						Constants.STRING_SERIALIZER);
		rssq.setColumnFamily(cfName);
		rssq.setKeys(keystart, keyend);
		rssq.setRange(rangestart, rangefinish, false, 5);
		QueryResult<OrderedSuperRows<String, String, String, String>> qr = rssq
				.execute();
		return qr.get();
	}
	
	/**
	 * To get a range of records/rows which contain specified super column data
	 * @param keyspace The Keyspace object
	 * @param cfName The column family name
	 * @param superColumnName The super column name
	 * @param keystart The start key value
	 * @param keyend The end key value
	 * @param rangestart The start range of columns
	 * @param rangefinish The end range of columns
	 * @return
	 */
	public OrderedRows<String, String, String> getRangeSubSliceQuery(
			Keyspace keyspace, String cfName, String superColumnName, String keystart,
			String keyend, String rangestart, String rangefinish) {
		RangeSubSlicesQuery<String, String, String, String> rssq = HFactory
				.createRangeSubSlicesQuery(keyspace,
						Constants.STRING_SERIALIZER,
						Constants.STRING_SERIALIZER,
						Constants.STRING_SERIALIZER,
						Constants.STRING_SERIALIZER);
		rssq.setColumnFamily(cfName);
		rssq.setSuperColumn(superColumnName);
		rssq.setKeys(keystart, keyend);
		// if we wanted all columns which has a common prefix than set that
		// prefix in rangestart and set "" in rangefinish
		// then first 3 columns in order will be returned
		rssq.setRange(rangestart, rangefinish, false, 3);
		QueryResult<OrderedRows<String, String, String>> qr = rssq.execute();
		return qr.get();
	}
	/**
	 * To delete a sub column of a super column
	 * @param keySpace The Keyspace object
	 * @param rowkeyid The row key of a record
	 * @param cfName The name of the column family
	 * @param scName The name of the super column
	 * @param sbName The name of the sub column
	 */
	public void deleteSubColumn(Keyspace keySpace, String rowkeyid,
			String cfName, String scName, String sbName) {
		Mutator<String> mutator = HFactory.createMutator(keySpace,
				Constants.STRING_SERIALIZER);
		mutator.addSubDelete(rowkeyid, cfName, scName, sbName,
				Constants.STRING_SERIALIZER, Constants.STRING_SERIALIZER);
		MutationResult mutationResult = mutator.execute();
		log.info("Delete SubColumn : " + sbName + " of " + scName + " :"
				+ mutationResult.toString());
	}
	
	/**
	 * To delete a Super Column
	 * @param keySpace The Keyspace object
	 * @param rowkeyid The row key of a record
	 * @param cfName The name of the column family
	 * @param scName The name of the super column
	 */
	public void deleteSuperColumn(Keyspace keySpace, String rowkeyid,
			String cfName, String scName) {

		Mutator<String> mutator = HFactory.createMutator(keySpace,
				Constants.STRING_SERIALIZER);
		mutator.addSuperDelete(rowkeyid, cfName, scName,
				Constants.STRING_SERIALIZER);
		MutationResult mutationResult = mutator.execute();
		log.info("Delete SuperColumn : " + scName + " :"
				+ mutationResult.toString());
	}
	
	// To build a HSuperColumn from a map object
	public HSuperColumn<String, String, String> getSuperColumnObjFromMap(
			Map<String, String> map, String superColumnName) {
		List<HColumn<String, String>> columnsList = new ArrayList<HColumn<String, String>>();
		for (Map.Entry<String, String> entry : map.entrySet()) {
			columnsList.add(HFactory.createStringColumn(entry.getKey(),
					entry.getValue()));
		}
		return HFactory.createSuperColumn(superColumnName, columnsList,
				Constants.STRING_SERIALIZER, Constants.STRING_SERIALIZER,
				Constants.STRING_SERIALIZER);
	}
	
	// To build a HSuperColumn from a set collection
	public HSuperColumn<String, String, String> getSuperColumnObjFromSet(
			Set<String> set, String superColumnName) {
		List<HColumn<String, String>> columnsList = new ArrayList<HColumn<String, String>>();
		for (String entry : set) {
			columnsList.add(HFactory.createStringColumn(entry, ""));
		}
		return HFactory.createSuperColumn(superColumnName, columnsList,
				Constants.STRING_SERIALIZER, Constants.STRING_SERIALIZER,
				Constants.STRING_SERIALIZER);
	}
}
