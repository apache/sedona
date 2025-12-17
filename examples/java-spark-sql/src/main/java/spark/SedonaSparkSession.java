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

import org.apache.sedona.spark.SedonaContext;
import org.apache.spark.sql.SparkSession;

public class SedonaSparkSession {

  public SparkSession session;

  public SedonaSparkSession() {

    // Set configuration for localhost spark cluster. Intended to be run from IDE or similar.
    // Use SedonaContext builder to create SparkSession with Sedona extensions
    SparkSession config =
        SedonaContext.builder()
            .appName(this.getClass().getSimpleName())
            .master("local[*]")
            .config("spark.ui.enabled", "false")
            .config(
                "spark.driver.extraJavaOptions",
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED")
            .getOrCreate();

    // Create Sedona-enabled SparkSession
    this.session = SedonaContext.create(config);
  }

  public SparkSession getSession() {
    // Access SparkSession object
    return this.session;
  }
}
