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
package spark;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SedonaParquetTest {

  protected static Properties properties;
  protected static String parquetPath;
  protected static SedonaSparkSession session;

  public SedonaParquetTest() {}

  @BeforeClass
  public static void setUpClass() throws IOException {

    session = new SedonaSparkSession();
    // Get parquetPath and any other application.properties
    try {
      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      Properties properties = new Properties();
      InputStream is = loader.getResourceAsStream("application.properties");
      properties.load(is);
      parquetPath = properties.getProperty("parquet.path");
    } catch (IOException e) {
      e.printStackTrace();
      parquetPath = "";
    }
  }

  @AfterClass
  public static void tearDownClass() {}

  @Before
  public void setUp() {}

  @Test
  public void connects() {
    assertNotNull("SparkSedonaSession not initialized correctly.", session);
    assertNotNull("Spark session not initialized inside SparkSedonaSession.", session.session);
  }

  @Test
  public void parquetAccessible() {
    File file = new File(parquetPath);
    assertTrue("Parquet file does not exist.", file.exists());
    assertTrue("Can't read geoparquet file on record.", file.canRead());
  }

  @Test
  public void canLoadRDD() {
    assertNotNull("Session is null.", session);
    Dataset<Row> insarDF = session.session.read().format("geoparquet").load(parquetPath);
    assertNotNull("Dataset was not created.", insarDF);
    assertTrue("Dataset is empty.", insarDF.count() > 0);
  }
}
