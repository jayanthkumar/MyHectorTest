package com.cassandra.services;

import java.util.List;

import org.apache.log4j.Logger;





import com.cassandra.constants.Constants;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;

public class Service {
	//private static final Logger log = LoggerFactory.getLogger(Service.class);
	private static final Logger log = Logger.getLogger(Service.class);
	
	/**
	 * Inserts a standard column 
	 * @param keyspace Keyspace object 
	 * @param rowKey Unique row key for a record
	 * @param cf The column family name
	 * @param column Hcolumn object to be inserted
	 */
	public void insertColumn(Keyspace keyspace, String rowKey, String cf, HColumn<String, String> column) {
		Mutator<String> mutator = HFactory.createMutator(keyspace, Constants.STRING_SERIALIZER);
		MutationResult mr = mutator.insert(rowKey, cf, column);
		//log.info("Inserted Column {} : {}" + new Object[] {column.getName() , mr.toString()});
		log.info("Insert Column "+column.getName()+ " : " + mr.toString());
	}
	
	/**
	 * To insert multiple columns in a row/record
	 * @param keyspace Keyspace object
	 * @param rowKey Unique row key for a record
	 * @param cf The column family name
	 * @param columns List of HColumn objects to be inserted in the specified row
	 */
	public void insertColumns(Keyspace keyspace, String rowKey, String cf, List<HColumn<String, String>> columns) {
		Mutator<String> mutator = HFactory.createMutator(keyspace, Constants.STRING_SERIALIZER);
		for(HColumn<String, String> col : columns) {
			mutator.addInsertion(rowKey, cf, col);
		}
		MutationResult mr = mutator.execute();
		log.info("Inserted Columns :" + mr.toString());
	}
	
	/**
	 * To get a column from a record/row
	 * @param keyspace Keyspace object
	 * @param cfName The column family name
	 * @param rowkeyid Unique row key for a record
	 * @param colName The column name to be fetch
	 * @return Returns HColumn object for the specified column name of a record
	 */
	public HColumn<String, String> getColumn(Keyspace keyspace, String cfName, String rowkeyid, String colName){
		ColumnQuery<String, String, String> columnQuery = HFactory.createColumnQuery(keyspace, Constants.STRING_SERIALIZER, Constants.STRING_SERIALIZER, Constants.STRING_SERIALIZER);
	    columnQuery.setColumnFamily(cfName);
	    columnQuery.setKey(rowkeyid);
	    columnQuery.setName(colName);
	    QueryResult<HColumn<String, String>> result = columnQuery.execute();
	    return result.get();
	}
	
	/**
	 * To get a range of columns for a specified row/record.Requires start range and end range for columns
	 * @param keyspace Keyspace object
	 * @param cfName The column family name
	 * @param rowkeyid Unique row key for a record
	 * @param start The start range for columns
	 * @param finish The finish range for columns
	 * @return Returns List of HColumn Objects for the specified record
	 */
	public List<HColumn<String, String>> getColumnsByRange(Keyspace keyspace, String cfName, String rowkeyid, String start, String finish) {
		SliceQuery<String, String, String> sq = HFactory.createSliceQuery(keyspace, Constants.STRING_SERIALIZER, Constants.STRING_SERIALIZER, Constants.STRING_SERIALIZER);
		sq.setColumnFamily(cfName);
		sq.setKey(rowkeyid);
		sq.setRange(start, finish, false, 100);
		QueryResult<ColumnSlice<String,String>> qr = sq.execute();
		return qr.get().getColumns();
	}
	
	/**
	 * To get a set of columnns for a specified row/record.Requires column name 
	 * @param keyspace Keyspace object
	 * @param cfName The column family name
	 * @param rowkeyid Unique row key for a record
	 * @param columnNames Column Names to be fetch
	 * @return Returns List of HColumn Objects for the specified record
	 */
	public List<HColumn<String, String>> getColumnsByName(Keyspace keyspace, String cfName, String rowkeyid, String... columnNames) {
		SliceQuery<String, String, String> sq = HFactory.createSliceQuery(keyspace, Constants.STRING_SERIALIZER, Constants.STRING_SERIALIZER, Constants.STRING_SERIALIZER);
		sq.setColumnFamily(cfName);
		sq.setKey(rowkeyid);
		sq.setColumnNames(columnNames);
		QueryResult<ColumnSlice<String,String>> qr = sq.execute();
		return qr.get().getColumns();
	}
	
	/**
	 * To retrieve multiple rows of data for a given set of keys
	 * 
	 * @param keyspace Keyspace object
	 * @param cf The column family name
	 * @param start The Start range of the columns
	 * @param finish The ending range of the columns
	 * @param keys row keys 
	 * @return Returns Rows Object which contain records of specified row keys 
	 */
	public Rows<String, String, String> getMultiSlice(Keyspace keyspace, String cf, String start, String finish, String... keys){
		MultigetSliceQuery<String, String, String> msq = HFactory.createMultigetSliceQuery(keyspace, Constants.STRING_SERIALIZER, Constants.STRING_SERIALIZER, Constants.STRING_SERIALIZER);
		msq.setColumnFamily(cf);
		msq.setKeys(keys);
		msq.setRange(start, finish, false, 5);
		QueryResult<Rows<String, String, String>>  qr = msq.execute();
		return qr.get();
	}
	
	// Note :  if you are using OPP in your storage configuration, the results returned from a RangeSlicesQuery will not be in any key order
	// to page over results rangeSlicesQuery.setKeys(orderedRows.peekLast().getKey(), "");
	/**
	 * 
	 * To get a range of records, RangeSlicesQuery uses a 
	 * contiguous range of keys as opposed to the specific 
	 * set of keys used by MultigetSliceQuery
	 * 
	 * @param keyspace Keyspace object
	 * @param cf The column family name
	 * @param keystart The start range for the keys
	 * @param keyend The end range for the keys
	 * @param rangestart The Start range of the columns
	 * @param rangefinish The ending range of the columns
	 * @return Returns OrderedRows Object which contain records in that specified range
	 */
	public OrderedRows<String, String, String> getRangeSlice(Keyspace keyspace, String cf, String keystart, String keyend, String rangestart, String rangefinish) {
		RangeSlicesQuery<String, String, String> rsq = HFactory.createRangeSlicesQuery(keyspace, Constants.STRING_SERIALIZER, Constants.STRING_SERIALIZER, Constants.STRING_SERIALIZER);
		rsq.setColumnFamily(cf);
		rsq.setKeys(keystart, keyend);
		// if wanted to return all columns starting with a common prefix rangestart should be set with that prefix
		// and rangefinish should be set with ""
		rsq.setRange(rangestart, rangefinish, false, 5);
		//
		rsq.setRowCount(11);
		// to retrieve a large number of keys and not really need or care about the columns to which they are associated.
		//rsq.setReturnKeysOnly();
		QueryResult<OrderedRows<String, String, String>> qr = rsq.execute();
		return qr.get();
	}
		
	/**
	 * Deletes column.If we set null in columnName then all columns in that row/record will be deleted
	 * @param keyspace keyspace Keyspace object
	 * @param rowkeyid Unique row key for a record
	 * @param cf The column family name
	 * @param columnName The column name to be deleted
	 */
	public void deleteColumn(Keyspace keyspace, String rowkeyid, String cf, String columnName) {
		Mutator<String> mutator = HFactory.createMutator(keyspace, Constants.STRING_SERIALIZER);
		mutator.addDeletion(rowkeyid, cf, columnName, Constants.STRING_SERIALIZER);
		MutationResult mr = mutator.execute();
		log.info("Deleted Column " + cf + ":" + mr.toString());
	}
	
	/*private void result(QueryResult qr) {
		System.out.println("Execution time: " + qr.getExecutionTimeMicro());
		System.out.println("CassandraHost used: " +qr.getHostUsed());
		System.out.println("Query Execute: " +qr.getQuery());
	}*/

}
