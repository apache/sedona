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
package org.apache.sedona.core.utils;

import static org.junit.Assert.*;

import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.Envelope;

public class SedonaConfTest {

  @BeforeClass
  public static void setUp() {
    SparkSession.builder().config("sedona.join.numpartition", "2").master("local").getOrCreate();
  }

  @AfterClass
  public static void tearDown() {
    SparkSession.active().sparkContext().stop();
  }

  @Test
  public void testRuntimeConf() {
    assertEquals(2, SedonaConf.fromActiveSession().getFallbackPartitionNum());
    SparkSession.active().conf().set("sedona.join.numpartition", "3");
    assertEquals(3, SedonaConf.fromActiveSession().getFallbackPartitionNum());
  }

  @Test
  public void testDatasetBoundary() {
    SparkSession.active().conf().set("sedona.join.boundary", "1,2,3,4");
    Envelope datasetBoundary = SedonaConf.fromActiveSession().getDatasetBoundary();
    assertEquals("Env[1.0 : 2.0, 3.0 : 4.0]", datasetBoundary.toString());
  }

  @Test
  public void testBytesFromString() {
    assertEquals(-1, SedonaConf.bytesFromString("-1"));
    assertEquals(1024, SedonaConf.bytesFromString("1k"));
    assertEquals(2097152, SedonaConf.bytesFromString("2MB"));
  }
}
