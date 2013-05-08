package com.strumsoft.cassandra.constants;

import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;

public interface Constants {
	// Configure all this properties before executing tests
	String CLUSTERNAME = "Strumsoft";
	String STRATEGYCLASS = org.apache.cassandra.locator.SimpleStrategy.class.getName();
	String KEYSPACE_NAME = "Test_ks";
	String HOST = "localhost:9160";
	int REPLICATIONFACTOR = 1;
	StringSerializer STRING_SERIALIZER = StringSerializer.get();
	CompositeSerializer COMPOSITESERIALIZER = CompositeSerializer.get();
	DynamicCompositeSerializer DYNAMICCOMPOSITESERIALIZER = DynamicCompositeSerializer.get();
}
