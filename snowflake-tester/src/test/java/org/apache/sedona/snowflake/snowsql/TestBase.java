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
package org.apache.sedona.snowflake.snowsql;

import static org.junit.Assert.assertArrayEquals;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import junit.framework.TestCase;
import org.apache.sedona.common.Constructors;
import org.apache.sedona.snowflake.snowsql.ddl.Constants;
import org.apache.sedona.snowflake.snowsql.ddl.UDFDDLGenerator;
import org.apache.sedona.snowflake.snowsql.ddl.UDTFDDLGenerator;
import org.junit.Ignore;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Ignore
public class TestBase extends TestCase {

  public SnowClient snowClient = null;

  private Map<String, String> buildDDLConfigs = null;

  Logger logger = LoggerFactory.getLogger(TestBase.class);

  private static boolean jarUploaded = false;

  private static String snowflake_db_name = null;

  public void registerUDF(String functionName, Class<?>... paramTypes) {
    try {
      String ddl =
          UDFDDLGenerator.buildUDFDDL(
              UDFs.class.getMethod(functionName, paramTypes),
              buildDDLConfigs,
              "@ApacheSedona",
              false,
              "");
      ResultSet res = snowClient.executeQuery(ddl);
      res.next();
      assert res.getString(1).contains("successfully created");
    } catch (SQLException | NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  public void registerUDFV2(String functionName, Class<?>... paramTypes) {
    Constants.snowflakeTypeMap.replace("Geometry", "GEOMETRY");
    try {
      String ddl =
          UDFDDLGenerator.buildUDFDDL(
              UDFsV2.class.getMethod(functionName, paramTypes),
              buildDDLConfigs,
              "@ApacheSedona",
              false,
              "");
      System.out.println(ddl);
      ResultSet res = snowClient.executeQuery(ddl);
      res.next();
      assert res.getString(1).contains("successfully created");
    } catch (SQLException | NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  public void registerUDFGeography(String functionName, Class<?>... paramTypes) {
    Constants.snowflakeTypeMap.replace("Geometry", "GEOGRAPHY");
    try {
      String ddl =
          UDFDDLGenerator.buildUDFDDL(
              UDFsV2.class.getMethod(functionName, paramTypes),
              buildDDLConfigs,
              "@ApacheSedona",
              false,
              "");
      ResultSet res = snowClient.executeQuery(ddl);
      res.next();
      assert res.getString(1).contains("successfully created");
    } catch (SQLException | NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  public void registerUDTF(Class<?> clz) {
    try {
      String ddl = UDTFDDLGenerator.buildUDTFDDL(clz, buildDDLConfigs, "@ApacheSedona", false, "");
      ResultSet res = snowClient.executeQuery(ddl);
      res.next();
      assert res.getString(1).contains("successfully created");
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public void init() throws SQLException {
    snowClient = SnowClient.newFromEnv();
    String sedonaVersion = System.getenv("SEDONA_VERSION");
    String geotoolsVersion = System.getenv("SNOWFLAKE_GEOTOOLS_VERSION");
    // init configs
    buildDDLConfigs = new HashMap<>();
    buildDDLConfigs.put(Constants.SEDONA_VERSION, sedonaVersion);
    buildDDLConfigs.put(Constants.GEOTOOLS_VERSION, geotoolsVersion);
    System.out.println("Using Snowflake DB: " + snowflake_db_name);
    // upload libraries
    if (!jarUploaded) {
      snowflake_db_name = "TMP_TESTDB_" + generateRandomString(8);
      System.out.println("Creating Snowflake DB: " + snowflake_db_name);
      // drop then create db to make sure test env fresh
      snowClient.executeQuery("drop database if exists " + snowflake_db_name);
      snowClient.executeQuery("create database " + snowflake_db_name);
      snowClient.executeQuery("use database " + snowflake_db_name);
      snowClient.executeQuery("create schema " + System.getenv("SNOWFLAKE_SCHEMA"));
      snowClient.executeQuery("use schema " + System.getenv("SNOWFLAKE_SCHEMA"));
      snowClient.executeQuery("CREATE STAGE ApacheSedona FILE_FORMAT = (COMPRESSION = NONE)");
      snowClient.uploadFile(
          String.format("tmp/sedona-snowflake-%s.jar", sedonaVersion), "ApacheSedona");
      snowClient.uploadFile(
          String.format("tmp/geotools-wrapper-%s.jar", geotoolsVersion), "ApacheSedona");
      jarUploaded = true;
    } else {
      System.out.println("Using Snowflake DB: " + snowflake_db_name);
      snowClient.executeQuery("use database " + snowflake_db_name);
      snowClient.executeQuery("use schema " + System.getenv("SNOWFLAKE_SCHEMA"));
    }
    registerDependantUDFs();
  }

  public void tearDown() {
    try {
      System.out.println("Dropping Snowflake DB: " + snowflake_db_name);
      snowClient.executeQuery("drop database if exists " + snowflake_db_name);
      jarUploaded = false;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    if (snowClient != null) {
      try {
        snowClient.close();
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public void registerDependantUDFs() {
    registerUDF("ST_GeomFromWKT", String.class);
    registerUDF("ST_GeomFromText", String.class);
    registerUDF("ST_AsText", byte[].class);
    registerUDF("ST_Point", double.class, double.class);
  }

  public void verifySqlSingleRes(String sql, Object expect) {
    try {
      ResultSet res = snowClient.executeQuery(sql);
      res.next();
      if (expect instanceof byte[]) {
        assertArrayEquals((byte[]) expect, (byte[]) res.getObject(1));
      } else if (expect instanceof Pattern) {
        String val = res.getString(1);
        assertTrue(((Pattern) expect).matcher(val).matches());
      } else if (expect instanceof List) {
        List expectList = (List) expect;
        for (int i = 0; i < expectList.size(); i++) {
          assertEquals(expectList.get(i), res.getObject(i + 1));
        }
      } else if (expect instanceof Integer) {
        assertEquals(expect, res.getInt(1));
      } else if (expect instanceof Geometry) {
        Geometry e = ((Geometry) expect);
        e.normalize();
        Geometry a = Constructors.geomFromWKT(res.getString(1), 0);
        a.normalize();
        assertEquals(e, a);
      } else {
        assertEquals(expect, res.getObject(1));
      }
    } catch (SQLException | ParseException e) {
      throw new RuntimeException(e);
    }
  }

  public ResultSet sqlSingleRes(String sql) {
    try {
      ResultSet res = snowClient.executeQuery(sql);
      res.next();
      return res;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Generates a random string of a specified length.
   *
   * @param length The length of the random string.
   * @return A random string consisting of alphabet letters only.
   */
  private static String generateRandomString(int length) {
    // Generate a string of all letters
    String chars =
        IntStream.concat(IntStream.rangeClosed('A', 'Z'), IntStream.rangeClosed('a', 'z'))
            .mapToObj(c -> "" + (char) c)
            .collect(Collectors.joining());

    Random random = new Random();

    // Build a random string from the full set of characters
    return random
        .ints(length, 0, chars.length())
        .mapToObj(i -> "" + chars.charAt(i)) // Correctly refer to character position
        .collect(Collectors.joining());
  }
}
