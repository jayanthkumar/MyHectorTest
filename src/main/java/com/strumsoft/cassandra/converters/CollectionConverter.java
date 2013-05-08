package com.strumsoft.cassandra.converters;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;

import org.apache.log4j.Logger;

import me.prettyprint.hom.PropertyMappingDefinition;
import me.prettyprint.hom.converters.Converter;

public class CollectionConverter implements Converter<Collection<?>> {
	private static final Logger log = Logger.getLogger(CollectionConverter.class);

	public Collection<?> convertCassTypeToObjType(PropertyMappingDefinition md, byte[] value) {
		Collection<?> collection = null;
		try {
			ByteArrayInputStream bis = new ByteArrayInputStream(value);
			ObjectInputStream ois = new ObjectInputStream(bis);
			collection = (Collection<?>) ois.readObject();
			log.debug("Fetching Collection object :" + collection);
		} catch (IOException ex) {
			log.info("IOException :" + ex.getLocalizedMessage());
		} catch (ClassNotFoundException ex) {
			log.info("ClassNotFoundException :" + ex.getLocalizedMessage());
		}
		return collection;
	}

	public byte[] convertObjTypeToCassType(Collection<?> value) {
		log.debug("Inserting Collection object :" + value);
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
