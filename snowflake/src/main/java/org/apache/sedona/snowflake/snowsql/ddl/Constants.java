/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sedona.snowflake.snowsql.ddl;

import java.util.HashMap;
import java.util.Map;

public class Constants {
  public static Map<String, String> snowflakeTypeMap = new HashMap<>();

  static {
    snowflakeTypeMap.put(String.class.getTypeName(), "VARCHAR");
    snowflakeTypeMap.put("String", "VARCHAR");
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
    snowflakeTypeMap.put("Geometry", "GEOMETRY");
  }

  public static String SEDONA_VERSION = "sedona_version";

  public static String GEOTOOLS_VERSION = "geotools_version";
}
