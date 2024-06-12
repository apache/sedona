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
package org.apache.sedona.core.spatialOperator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.sedona.core.TestBase;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

public class SpatialQueryTestBase extends TestBase {
  protected static JavaRDD<Geometry> inputRdd;
  protected static Map<Integer, Geometry> testDataset;

  protected static void initialize(String testName, String testDataPath) throws IOException {
    initialize(testName);
    sc.setLogLevel("ERROR");
    inputRdd = readTestDataAsRDD(testDataPath);
    testDataset = readTestDataAsMap(testDataPath);
  }

  private static Geometry lineToGeometry(String line) {
    String[] lineSplit = line.split("\t");
    WKTReader reader = new WKTReader();
    try {
      Geometry geom = reader.read(lineSplit[2]);
      geom.setUserData(new RangeQueryTest.UserData(Integer.parseInt(lineSplit[0]), lineSplit[1]));
      return geom;
    } catch (ParseException e) {
      throw new RuntimeException("Invalid geometry data in line: " + line, e);
    }
  }

  protected static JavaRDD<Geometry> readTestDataAsRDD(String fileName) {
    String inputLocation = RangeQueryTest.class.getClassLoader().getResource(fileName).getPath();
    return sc.textFile("file://" + inputLocation).map(SpatialQueryTestBase::lineToGeometry);
  }

  protected static Map<Integer, Geometry> readTestDataAsMap(String fileName) throws IOException {
    String inputLocation = RangeQueryTest.class.getClassLoader().getResource(fileName).getPath();
    try (Stream<String> lines = Files.lines(Paths.get(inputLocation))) {
      Map<Integer, Geometry> map = new HashMap<>();
      lines.forEach(
          line -> {
            Geometry geom = lineToGeometry(line);
            int id = ((RangeQueryTest.UserData) geom.getUserData()).getId();
            map.put(id, geom);
          });
      return map;
    }
  }

  protected static boolean evaluateSpatialPredicate(
      SpatialPredicate spatialPredicate, Geometry geom1, Geometry geom2) {
    switch (spatialPredicate) {
      case INTERSECTS:
        return geom1.intersects(geom2);
      case CONTAINS:
        return geom1.contains(geom2);
      case WITHIN:
        return geom1.within(geom2);
      case COVERS:
        return geom1.covers(geom2);
      case COVERED_BY:
        return geom1.coveredBy(geom2);
      case OVERLAPS:
        return geom1.overlaps(geom2);
      case CROSSES:
        return geom1.crosses(geom2);
      case EQUALS:
        return geom1.equals(geom2);
      case TOUCHES:
        return geom1.touches(geom2);
      default:
        throw new IllegalArgumentException("Unknown spatial predicate: " + spatialPredicate);
    }
  }

  protected static class UserData {
    private final int id;
    private final String name;

    public UserData(int id, String name) {
      this.id = id;
      this.name = name;
    }

    public int getId() {
      return id;
    }

    public String getName() {
      return name;
    }

    @Override
    public int hashCode() {
      return id;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof RangeQueryTest.UserData)) {
        return false;
      }
      RangeQueryTest.UserData other = (RangeQueryTest.UserData) obj;
      return id == other.id && name.equals(other.name);
    }
  }
}
