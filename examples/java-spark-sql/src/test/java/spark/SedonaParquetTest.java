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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class SedonaParquetTest {


    protected static Properties properties;
    protected static String parquetPath;
    protected static SedonaSparkSession session;

    public SedonaParquetTest() {
    }

    @BeforeAll
    public static void setUpClass() throws IOException {

        session = new SedonaSparkSession();
        //Get parquetPath and any other application.properties
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

    @AfterAll
    public static void tearDownClass() {
    }

    @BeforeEach
    public void setUp() {
    }

    @AfterEach
    public void tearDown() {
    }

    @Test
    public void connects() {
        assertNotNull(session, "SparkSedonaSession not initialized correctly.");
        assertNotNull(session.session, "Spark session not initialized inside SparkSedonaSession.");
    }

    @Test
    public void parquetAccessible() {
        File file = new File(parquetPath);
        assertTrue(file.exists(), "Parquet file does not exist.");
        assertTrue(file.canRead(), "Can't read geoparquet file on record.");
    }

    @Test
    public void canLoadRDD() {
        assertNotNull(session, "Session is null.");
        Dataset<Row> insarDF = session.session.read()
                .format("geoparquet")
                .load(parquetPath);
        assertNotNull(insarDF, "Dataset was not created.");
        assertTrue(insarDF.count() > 0, "Dataset is empty.");
    }

}
