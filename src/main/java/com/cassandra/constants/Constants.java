package com.cassandra.constants;

import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;

/**
 * 
 * @author Jayanth Kumar
 *
 */
public interface Constants {
	// Configure all this properties before executing tests
	String CLUSTERNAME = "coolCluster";
	String STRATEGYCLASS = org.apache.cassandra.locator.SimpleStrategy.class.getName();
	String KEYSPACE_NAME = "Test_ks";
	String HOST = "localhost:9160";
	int REPLICATIONFACTOR = 1;
	StringSerializer STRING_SERIALIZER = StringSerializer.get();
	CompositeSerializer COMPOSITESERIALIZER = CompositeSerializer.get();
	DynamicCompositeSerializer DYNAMICCOMPOSITESERIALIZER = DynamicCompositeSerializer.get();
}
