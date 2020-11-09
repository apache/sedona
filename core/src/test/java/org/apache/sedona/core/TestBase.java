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

package org.apache.sedona.core;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.sedona.core.serde.SedonaKryoRegistrator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;

public class TestBase
{
    protected static SparkConf conf;
    protected static JavaSparkContext sc;

    protected static void initialize(final String testSuiteName)
    {
        conf = new SparkConf().setAppName(testSuiteName).setMaster("local[2]");
        conf.set("spark.serializer", KryoSerializer.class.getName());
        conf.set("spark.kryo.registrator", SedonaKryoRegistrator.class.getName());

        sc = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
    }
}

