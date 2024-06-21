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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;
import net.snowflake.client.jdbc.SnowflakeConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnowClient {

  private Properties props = null;

  private Connection conn = null;

  private String connUrl = null;

  private static Logger logger = LoggerFactory.getLogger(SnowClient.class);

  public SnowClient(Properties props, String connUrl) throws SQLException {
    this.props = props;
    this.connUrl = connUrl;
    this.initConnection();
  }

  public void initConnection() throws SQLException {
    if (conn == null || conn.isClosed()) {
      System.out.println("connect to " + connUrl);
      conn = DriverManager.getConnection(connUrl, props);
    }
  }

  public void close() throws SQLException {
    try {
      if (conn != null && !conn.isClosed()) {
        conn.close();
      }
    } finally {
      conn = null;
    }
  }

  public ResultSet executeQuery(String query) throws SQLException {
    logger.info("execute query " + query);
    Objects.requireNonNull(conn);
    ResultSet res = conn.createStatement().executeQuery(query);
    return res;
  }

  public void uploadFile(String path, String stage) {
    File jarFile = new File(path);
    String[] pList = path.split("/");
    String fileName = pList[pList.length - 1];
    try {
      System.out.printf("upload file %s to stage %s%n", path, stage);
      logger.info(String.format("upload file %s to stage %s", path, stage));
      FileInputStream fileInputStream = new FileInputStream(jarFile);
      conn.unwrap(SnowflakeConnection.class)
          .uploadStream(stage, "", fileInputStream, fileName, false);
      logger.info("upload finished");
    } catch (FileNotFoundException | SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public static SnowClient newFromEnv() throws SQLException {
    Properties prop = new Properties();
    // check auth method
    String authMethod = System.getenv("SNOWFLAKE_AUTH_METHOD");
    if (authMethod.equals("BASIC")) {
      prop.put("user", System.getenv("SNOWFLAKE_USER"));
      prop.put("password", System.getenv("SNOWFLAKE_PASSWORD"));
    }
    prop.put("schema", System.getenv("SNOWFLAKE_SCHEMA"));
    prop.put("warehouse", System.getenv("SNOWFLAKE_WAREHOUSE"));
    prop.put("role", System.getenv("SNOWFLAKE_ROLE"));
    String accountName = System.getenv("SNOWFLAKE_ACCOUNT");
    String connectionUrl =
        String.format(
            "jdbc:snowflake://%s.snowflakecomputing.com/?query_tag='ApacheSedona'", accountName);
    return new SnowClient(prop, connectionUrl);
  }
}
