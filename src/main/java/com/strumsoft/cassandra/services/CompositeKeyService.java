package com.strumsoft.cassandra.services;

import java.util.List;

import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.log4j.Logger;

/**
 * 
 * @author shiva
 *
 */
		
public class CompositeKeyService {
	private static Logger log = Logger.getLogger(CompositeKeyService.class);
	private StringSerializer stringSerializer = StringSerializer.get();
	private CompositeSerializer compositeSerializer = CompositeSerializer.get();
	private DynamicCompositeSerializer dynamicCompositeSerializer = DynamicCompositeSerializer.get();
	
	/**
	 * Insert single composite key
	 * 
	 * @param keyspace
	 * @param key
	 * @param cf
	 * @param sc
	 */
	public void insertCompositeKey(Keyspace keyspace, String key, String cf,
			HColumn<Composite, String> sc) {
		Mutator<String> mutator = HFactory.createMutator(keyspace,
				stringSerializer);
		mutator.addInsertion(key, cf, sc);
		mutator.execute();
		log.info("Created compoite key");
	}

	/**
	 * Insert multiple composite keys Fixed number and order defined in column
	 * configuration.
	 * Static composites have no way to do decode as there is no
	 * type information.- static composite
	 * 
	 * @param keyspace
	 * @param key
	 * @param cf
	 * @param cks
	 */
	public void insertMultipleCompositeKey(Keyspace keyspace, String key, String cf,
			List<HColumn<Composite, String>> cks) {
		Mutator<String> mutator = HFactory.createMutator(keyspace,
				stringSerializer);
		for (HColumn<Composite, String> ck : cks) {
			log.debug("Composite key {}:"+ck);
			mutator.addInsertion(key, cf, ck);
		}
		mutator.execute();
		log.info("Created multiple composite keys "+cks);
	}

	/**
	 * Insert multiple dynamic composite keys Any number and order of types at
	 * runtime
	 * To achieve dynamic composites, Hector writes additional type
	 * data into the byte array, so that it knows how to decode the bytes when
	 * it reads them back out - Dynamic composite key
	 * 
	 * @param keyspace
	 * @param key
	 * @param cf
	 * @param cks
	 */
	public void insertMultipleDynamicCompositeKey(Keyspace keyspace, String key, String cf,
			List<HColumn<DynamicComposite, String>> dcks) {
		Mutator<String> mutator = HFactory.createMutator(keyspace,
				stringSerializer);
		for (HColumn<DynamicComposite, String> dck : dcks) {
			log.debug("Dynamic Composite key {} :"+dck);
			mutator.addInsertion(key, cf, dck);
		}
		mutator.execute();
		log.info("Created multiple dynamic composite keys "+dcks);
	}
	
	/**
	 * Read composite keys based on range
	 * @param keyspace
	 * @param cf
	 * @param key
	 * @param start
	 * @param end
	 * @return
	 */
	public List<HColumn<Composite, String>> readAllCompositeKeyByRange(
			Keyspace keyspace, String cf, String key, Composite start,
			Composite end) {
		SliceQuery<String, Composite, String> sliceQuery = HFactory
				.createSliceQuery(keyspace, stringSerializer,
						compositeSerializer, stringSerializer);
		sliceQuery.setColumnFamily(cf);
		sliceQuery.setKey(key);
		sliceQuery.setRange(start, end, false, 1000);
		List<HColumn<Composite, String>> result = sliceQuery.execute().get().getColumns();
		log.debug("Read all Composite keys :"+result);
		return result;
	}
	
	/**
	 * Read dynamic composite keys based on range
	 * 
	 * @param keyspace
	 * @param cf
	 * @param key
	 * @param start
	 * @param end
	 * @return
	 */
	public List<HColumn<DynamicComposite, String>> readAllDynamicCompositeKeyByRange(
			Keyspace keyspace, String cf, String key, DynamicComposite start,
			DynamicComposite end) {
		SliceQuery<String, DynamicComposite, String> sliceQuery = HFactory
				.createSliceQuery(keyspace, stringSerializer,
						dynamicCompositeSerializer, stringSerializer);
		sliceQuery.setColumnFamily(cf);
		sliceQuery.setKey(key);
		sliceQuery.setRange(start, end, false, 1000);
		List<HColumn<DynamicComposite, String>> result = sliceQuery.execute().get().getColumns();
		log.debug("Read all dynamic Composite keys :"+result);
		return result;
	}
	
	/**
	 * Update all composite keys for a row
	 * @param keyspace
	 * @param key
	 * @param cf
	 * @param cks
	 */
	public void updateMutliCompositeKeys(Keyspace keyspace, String key, String cf, List<HColumn<Composite, String>> cks) {
		Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
		for (HColumn<Composite, String> ck : cks) {
			mutator.addInsertion(key, cf, ck);
		}
		mutator.execute();
		log.info("Updated all composite keys "+cks);
	}
	
	/**
	 * Delete all composite keys for a row
	 * 
	 * @param keyspace
	 * @param key
	 * @param cf
	 */
	public void deleteAllCompositeKeys(Keyspace keyspace, String key, String cf, Composite comp) {
		Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
		mutator.addDeletion(key, cf, comp, CompositeSerializer.get());
		mutator.execute();
		log.info("Deleted all composite keys ");
	}
}
