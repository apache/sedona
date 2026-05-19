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
package org.apache.sedona.flink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ModuleTest extends TestBase {

  @BeforeClass
  public static void onceExecutedBeforeAll() {
    initialize();
    tableEnv.executeSql("LOAD MODULE sedona");
  }

  @AfterClass
  public static void onceExecutedAfterAll() {
    tableEnv.executeSql("UNLOAD MODULE sedona");
  }

  @Test
  public void testSedonaModuleIsListed() throws Exception {
    TableResult result = tableEnv.executeSql("SHOW MODULES");

    List<String> loadedModules = new ArrayList<>();
    try (CloseableIterator<Row> it = result.collect()) {
      while (it.hasNext()) {
        Row row = it.next();
        if (row.getField(0) != null) {
          loadedModules.add(row.getField(0).toString().trim());
        }
      }
    }

    boolean isSedonaLoaded =
        loadedModules.stream().anyMatch(moduleName -> moduleName.equalsIgnoreCase("sedona"));

    assertTrue("Module 'sedona' should be listed in SHOW MODULES", isSedonaLoaded);

    TableResult fullResult = tableEnv.executeSql("SHOW FULL MODULES");
    boolean foundAndUsed = false;
    try (CloseableIterator<Row> it = fullResult.collect()) {
      while (it.hasNext()) {
        Row row = it.next();
        String moduleName = row.getField(0).toString().trim();
        boolean isUsed = (Boolean) row.getField(1);
        if (moduleName.equalsIgnoreCase("sedona") && isUsed) {
          foundAndUsed = true;
          break;
        }
      }
    }

    assertTrue("Module 'sedona' should be listed as 'used' in SHOW FULL MODULES", foundAndUsed);
  }

  @Test
  public void testRegularFunctionsAreLoaded() throws Exception {
    Set<String> loadedFunctions = getLoadedFunctions();

    for (org.apache.flink.table.functions.UserDefinedFunction func :
        org.apache.sedona.flink.Catalog.getFuncs()) {
      String funcName = func.getClass().getSimpleName().toLowerCase();
      assertTrue("Function " + funcName + " should be loaded", loadedFunctions.contains(funcName));
    }
  }

  @Test
  public void testPredicateFunctionsAreLoaded() throws Exception {
    Set<String> loadedFunctions = getLoadedFunctions();

    for (org.apache.flink.table.functions.UserDefinedFunction func :
        org.apache.sedona.flink.Catalog.getPredicates()) {
      String funcName = func.getClass().getSimpleName().toLowerCase();
      assertTrue("Predicate " + funcName + " should be loaded", loadedFunctions.contains(funcName));
    }
  }

  @Test
  public void testAggregateFunction() throws Exception {
    Table pointTable = createPointTable(testDataSize);
    tableEnv.createTemporaryView(pointTableName, pointTable);

    Table result =
        tableEnv.sqlQuery(
            "SELECT ST_AsText(ST_Envelope_Aggr(" + pointColNames[0] + ")) FROM " + pointTableName);
    Row row = last(result);
    String wkt = (String) row.getField(0);

    String expectedWkt =
        String.format(
            "POLYGON ((0 0, 0 %s, %s %s, %s 0, 0 0))",
            testDataSize - 1, testDataSize - 1, testDataSize - 1, testDataSize - 1);
    assertEquals(expectedWkt, wkt);

    tableEnv.dropTemporaryView(pointTableName);
  }

  @Test
  public void testConstructor() throws Exception {
    Table table = tableEnv.sqlQuery("SELECT ST_AsText(ST_Point(1.0, 2.0))");
    Row row = first(table);
    String wkt = (String) row.getField(0);
    assertEquals("POINT (1 2)", wkt);
  }

  @Test
  public void testMeasurementFunction() throws Exception {
    Table table =
        tableEnv.sqlQuery("SELECT ST_Area(ST_GeomFromText('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'))");
    Row row = first(table);
    Double area = (Double) row.getField(0);
    assertEquals(4.0, area, 0.0001);
  }

  @Test
  public void testPredicateFunction() throws Exception {
    Table table =
        tableEnv.sqlQuery(
            "SELECT ST_Intersects("
                + "ST_GeomFromText('POINT(1 1)'), "
                + "ST_GeomFromText('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))')"
                + ")");
    Row row = first(table);
    Boolean intersects = (Boolean) row.getField(0);
    assertTrue("Point should intersect the polygon", intersects);
  }

  private Set<String> getLoadedFunctions() throws Exception {
    TableResult result = tableEnv.executeSql("SHOW FUNCTIONS");
    Set<String> loadedFunctions = new HashSet<>();

    try (CloseableIterator<Row> it = result.collect()) {
      while (it.hasNext()) {
        Row row = it.next();
        if (row.getField(0) != null) {
          loadedFunctions.add(row.getField(0).toString().trim().toLowerCase());
        }
      }
    }
    return loadedFunctions;
  }

  static void initialize() {
    initialize(false);
  }

  static void initialize(boolean enableWebUI) {
    env =
        enableWebUI
            ? StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration())
            : StreamExecutionEnvironment.getExecutionEnvironment();
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    tableEnv = StreamTableEnvironment.create(env, settings);
  }
}
