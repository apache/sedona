package org.apache.sedona.core.io.avro.schema;

import org.apache.avro.Schema;
import org.apache.sedona.core.io.avro.SchemaUtils;
import org.apache.sedona.core.utils.SedonaUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.lang.reflect.Field;
import java.util.HashMap;

public class BaseSchemaTest {
    public static final String TEST_NAMESPACE = "com.apache.sedona";
    public static final String TEST_NAME = "dummy";
    @After
    public void removeParser() throws NoSuchFieldException, IllegalAccessException {
        Field field = SchemaUtils.SchemaParser.class.getDeclaredField("parser");
        field.setAccessible(true);
        Field typeMap = SchemaUtils.SchemaParser.class.getDeclaredField("dataTypes");
        typeMap.setAccessible(true);
        field.set(null,null);
        typeMap.set(null,null);
    }
    
}
