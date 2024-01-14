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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.Assert.assertArrayEquals;


@Ignore
public class TestBase extends TestCase {

    public SnowClient snowClient = null;

    private Map<String, String> buildDDLConfigs = null;

    Logger logger = LoggerFactory.getLogger(TestBase.class);

    private static boolean jarUploaded = false;

    public void registerUDF(String functionName, Class<?> ... paramTypes) {
        try {
            String ddl = UDFDDLGenerator.buildUDFDDL(UDFs.class.getMethod(
                    functionName,
                    paramTypes
            ), buildDDLConfigs, "@ApacheSedona", false, "");
            System.out.println(ddl);
            ResultSet res = snowClient.executeQuery(ddl);
            res.next();
            assert res.getString(1).contains("successfully created");
        } catch (SQLException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    public void registerUDFV2(String functionName, Class<?> ... paramTypes) {
        try {
            String ddl = UDFDDLGenerator.buildUDFDDL(UDFsV2.class.getMethod(
                    functionName,
                    paramTypes
            ), buildDDLConfigs, "@ApacheSedona", false, "");
            System.out.println(ddl);
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
            System.out.println(ddl);
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
        // upload libraries
        if (!jarUploaded) {
            // drop then create db to make sure test env fresh
            snowClient.executeQuery("drop database if exists " + System.getenv("SNOWFLAKE_DB"));
            snowClient.executeQuery("create database " + System.getenv("SNOWFLAKE_DB"));
            snowClient.executeQuery("use database " + System.getenv("SNOWFLAKE_DB"));
            snowClient.executeQuery("create schema " + System.getenv("SNOWFLAKE_SCHEMA"));
            snowClient.executeQuery("use schema " + System.getenv("SNOWFLAKE_SCHEMA"));
            snowClient.executeQuery("CREATE STAGE ApacheSedona FILE_FORMAT = (COMPRESSION = NONE)");
            snowClient.uploadFile(String.format("tmp/sedona-snowflake-%s.jar", sedonaVersion), "ApacheSedona");
            snowClient.uploadFile(String.format("tmp/geotools-wrapper-%s.jar", geotoolsVersion), "ApacheSedona");
            jarUploaded = true;
        }
        registerDependantUDFs();
    }

    public void tearDown() {
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
                assertArrayEquals( (byte[]) expect, (byte[]) res.getObject(1));
            } else if (expect instanceof Pattern) {
                String val = res.getString(1);
                assertTrue(((Pattern) expect).matcher(val).matches());
            } else if (expect instanceof List) {
                List expectList = (List) expect;
                for (int i = 0;i < expectList.size(); i++) {
                    assertEquals(expectList.get(i), res.getObject(i+1));
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
}
