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

import org.apache.spark.sql.SparkSession;
import org.apache.sedona.sql.utils.SedonaSQLRegistrator;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.RuntimeConfig;


public class SedonaSparkSession {
    
    private SparkConf conf;
    private RuntimeConfig config;
    public SparkSession session;

    public SedonaSparkSession() {

        //Set configuration for localhost spark cluster. Intended ot be run from IDE or similar.
        this.conf = new SparkConf().setAppName(this.getClass().getSimpleName())
                                   .setMaster("local[*]")
                                   .set("spark.ui.enabled", "false") 
                                   .set("spark.driver.extraJavaOptions",
                                        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED");
        this.session = SparkSession.builder()
                                    .config(this.conf)
                                    .getOrCreate();
        //Configure Saprk session to execute with Sedona
        SedonaSQLRegistrator.registerAll(this.session);
        this.config = this.session.conf();
    }

    public SparkSession getSession() {
        // Access SparkSession object 
        return this.session;
    }
    
}

