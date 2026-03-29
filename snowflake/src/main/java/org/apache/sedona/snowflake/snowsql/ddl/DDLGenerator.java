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

import static java.lang.System.exit;

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DDLGenerator {

  private static final Logger log = LoggerFactory.getLogger(DDLGenerator.class);

  public static Map<String, String> parseArgs(String[] args) {
    Map<String, String> argMap = new HashMap<>();
    if (args.length == 0) {
      printUsage();
      exit(0);
    }
    for (int i = 0; i < args.length; i++) {
      String arg = args[i];
      if (arg.equals("-h")) {
        printUsage();
        exit(0);
      }
      if (arg.startsWith("--")) {
        String argName = arg.substring(2).replace("-", "_");
        String argValue = args[++i];
        argMap.put(argName, argValue);
      }
    }
    // add sedona version to argMap. This fetches value from pom.xml and only works when running
    // from the terminal
    argMap.put(
        Constants.SEDONA_VERSION,
        DDLGenerator.class.getPackage().getImplementationVersion() == null
            ? "unknown"
            : DDLGenerator.class.getPackage().getImplementationVersion());
    try {
      assert argMap.containsKey(Constants.GEOTOOLS_VERSION);
    } catch (AssertionError e) {
      log.error("Missing required arguments");
      printUsage();
    }
    return argMap;
  }

  public static void printUsage() {
    log.info("Usage: java -jar snowflake/target/sedona-snowflake-1.5.1.jar [options]");
    log.info("Must have Arguments");
    log.info("  --geotools-version <version>");
    log.info("Optional have Arguments");
    log.info("  --schema <schema>  register functions to this schema. Default to sedona");
    log.info(
        "  --stageName <stageName>  snowflake stage name to upload jar files. Not needed if isNativeApp is true");
    log.info(
        "  --isNativeApp <true/false>  whether to generate DDL for Snowflake Native App. Default to false");
    log.info(
        "  --appRoleName <appRoleName>  application role name. Required when isNativeApp is true. Default to app_public");
    log.info("  --h  Print this help message");
    exit(0);
  }

  public static void main(String[] args) {
    String stageName;
    boolean isNativeApp;
    String appRoleName;

    Map<String, String> argMap = parseArgs(args);

    // check if isNativeApp. If so, set stageName to empty string since it is not needed. Also set
    // appRoleName.
    // If appRoleName is not provided, default to app_public
    if (argMap.getOrDefault("isNativeApp", "false").equals("true")) {
      isNativeApp = true;
      argMap.put("stageName", "");
      appRoleName = argMap.getOrDefault("appRoleName", "app_public");
      if (!argMap.containsKey("appRoleName")) {
        log.info(
            "-- AppRoleName is required when isNativeApp is true. If not provided, default to app_public");
      }
      stageName = "";
      log.info("-- Generating DDL for Snowflake Native App");
      log.info("CREATE APPLICATION ROLE IF NOT EXISTS {};", appRoleName);
      log.info("CREATE OR ALTER VERSIONED SCHEMA sedona;");
      log.info("GRANT USAGE ON SCHEMA sedona TO APPLICATION ROLE {};", appRoleName);
    } else {
      // If isNativeApp is false, set stageName to the provided value. Also set appRoleName to empty
      // string since it is not needed.
      // If stageName is not provided, default to @ApacheSedona. The name must start with @.
      isNativeApp = false;
      appRoleName = "";
      log.info("-- IsNativeApp is false. Generating DDL for User-Managed Snowflake Account");
      stageName = argMap.getOrDefault("stageName", "@ApacheSedona");
      if (!stageName.startsWith("@")) {
        log.error("-- StageName must start with @");
        exit(0);
      }
    }
    try {
      log.info("-- UDFs --");
      log.info(
          String.join("\n", UDFDDLGenerator.buildAll(argMap, stageName, isNativeApp, appRoleName)));
      log.info("-- UDTFs --");
      log.info(
          String.join(
              "\n", UDTFDDLGenerator.buildAll(argMap, stageName, isNativeApp, appRoleName)));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
