package org.apache.sedona.snowflake.snowsql.ddl;

import java.util.HashMap;
import java.util.Map;

public class Constants {
    public static Map<String, String> snowflakeTypeMap = new HashMap<>();

    static {
        snowflakeTypeMap.put(String.class.getTypeName(), "VARCHAR");
        snowflakeTypeMap.put(byte[].class.getTypeName(), "BINARY");
        snowflakeTypeMap.put(long.class.getTypeName(), "NUMBER");
        snowflakeTypeMap.put(Long.class.getTypeName(), "NUMBER");
        snowflakeTypeMap.put(Integer.class.getTypeName(), "NUMBER");
        snowflakeTypeMap.put(int.class.getTypeName(), "NUMBER");
        snowflakeTypeMap.put(long[].class.getTypeName(), "ARRAY");
        snowflakeTypeMap.put(String[].class.getTypeName(), "ARRAY");
        snowflakeTypeMap.put(boolean.class.getTypeName(), "BOOLEAN");
        snowflakeTypeMap.put(Boolean.class.getTypeName(), "BOOLEAN");
        snowflakeTypeMap.put(double.class.getTypeName(), "DOUBLE");
        snowflakeTypeMap.put(Double.class.getTypeName(), "DOUBLE");
        snowflakeTypeMap.put(float.class.getTypeName(), "FLOAT");
        snowflakeTypeMap.put(Float.class.getTypeName(), "FLOAT");
    }

    public static String SEDONA_VERSION = "sedona_version";

    public static String GEOTOOLS_VERSION = "geotools_version";
}
