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
package org.apache.sedona.common.utils;

import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

public class TelemetryCollector {

  private static final String BASE_URL = "https://sedona.gateway.scarf.sh/packages/";

  public static String send(String engineName, String language) {
    HttpURLConnection conn = null;
    String telemetrySubmitted = "";
    try {
      String arch = URLEncoder.encode(System.getProperty("os.arch").replaceAll(" ", "_"), "UTF-8");
      String os = URLEncoder.encode(System.getProperty("os.name").replaceAll(" ", "_"), "UTF-8");
      String jvm =
          URLEncoder.encode(System.getProperty("java.version").replaceAll(" ", "_"), "UTF-8");

      // Construct URL
      telemetrySubmitted =
          BASE_URL + language + "/" + engineName + "/" + arch + "/" + os + "/" + jvm;

      // Check for user opt-out
      if (System.getenv("SCARF_NO_ANALYTICS") != null
              && System.getenv("SCARF_NO_ANALYTICS").equals("true")
          || System.getenv("DO_NOT_TRACK") != null && System.getenv("DO_NOT_TRACK").equals("true")
          || System.getProperty("SCARF_NO_ANALYTICS") != null
              && System.getProperty("SCARF_NO_ANALYTICS").equals("true")
          || System.getProperty("DO_NOT_TRACK") != null
              && System.getProperty("DO_NOT_TRACK").equals("true")) {
        return telemetrySubmitted;
      }

      // Send GET request
      URL url = new URL(telemetrySubmitted);
      conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("GET");
      conn.connect();
      int responseCode = conn.getResponseCode();
      // Optionally check the response for successful execution
      if (responseCode != 200) {
        // Silent handling, no output or log
      }
    } catch (Exception e) {
      // Silent catch block
    } finally {
      if (conn != null) {
        conn.disconnect();
      }
    }
    return telemetrySubmitted;
  }
}
