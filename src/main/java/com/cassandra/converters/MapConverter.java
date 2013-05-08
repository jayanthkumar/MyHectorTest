package com.cassandra.converters;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;

import org.apache.log4j.Logger;

import me.prettyprint.hom.PropertyMappingDefinition;
import me.prettyprint.hom.converters.Converter;

/**
 * 
 * @author Jayanth Kumar
 *
 */
public class MapConverter implements Converter<Map<?, ?>>{
	
	private static final Logger log = Logger.getLogger(MapConverter.class);

	public Map<?, ?> convertCassTypeToObjType(PropertyMappingDefinition md,
			byte[] value) {
		Map<?, ?> map = null;
		try {
			ByteArrayInputStream bis = new ByteArrayInputStream(value);
			ObjectInputStream ois = new ObjectInputStream(bis);
			map = (Map<?, ?>) ois.readObject();
			log.debug("Fetching Map Result :" + map);
		} catch (IOException ex) {
			log.info("IOException :" + ex.getLocalizedMessage());
		} catch (ClassNotFoundException ex) {
			log.info("ClassNotFoundException :" + ex.getLocalizedMessage());
		}
		return map;
	}

	public byte[] convertObjTypeToCassType(Map<?, ?> value) {
		log.debug("Inserting Map object :" + value);
		ByteArrayOutputStream b = null;
		ObjectOutputStream o = null;
		try {
			b = new ByteArrayOutputStream();
			o = new ObjectOutputStream(b);
			o.writeObject(value);
			o.close();
		} catch (IOException e) {
			log.info("IOException :" + e.getLocalizedMessage());
		}
		return b.toByteArray();
	}

}
