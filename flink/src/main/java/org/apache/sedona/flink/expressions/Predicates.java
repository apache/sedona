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
package org.apache.sedona.flink.expressions;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Geometry;

public class Predicates {

  public static class ST_Intersects extends ScalarFunction {
    /** Constructor for relation checking without duplicate removal */
    public ST_Intersects() {}

    @DataTypeHint("Boolean")
    public Boolean eval(
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
            Object o1,
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
            Object o2) {
      Geometry geom1 = (Geometry) o1;
      Geometry geom2 = (Geometry) o2;
      return org.apache.sedona.common.Predicates.intersects(geom1, geom2);
    }
  }

  public static class ST_Contains extends ScalarFunction {

    /** Constructor for relation checking without duplicate removal */
    public ST_Contains() {}

    @DataTypeHint("Boolean")
    public Boolean eval(
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
            Object o1,
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
            Object o2) {
      Geometry geom1 = (Geometry) o1;
      Geometry geom2 = (Geometry) o2;
      return org.apache.sedona.common.Predicates.contains(geom1, geom2);
    }
  }

  public static class ST_Within extends ScalarFunction {
    /** Constructor for relation checking without duplicate removal */
    public ST_Within() {}

    @DataTypeHint("Boolean")
    public Boolean eval(
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
            Object o1,
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
            Object o2) {
      Geometry geom1 = (Geometry) o1;
      Geometry geom2 = (Geometry) o2;
      return org.apache.sedona.common.Predicates.within(geom1, geom2);
    }
  }

  public static class ST_Covers extends ScalarFunction {

    /** Constructor for relation checking without duplicate removal */
    public ST_Covers() {}

    @DataTypeHint("Boolean")
    public Boolean eval(
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
            Object o1,
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
            Object o2) {
      Geometry geom1 = (Geometry) o1;
      Geometry geom2 = (Geometry) o2;
      return org.apache.sedona.common.Predicates.covers(geom1, geom2);
    }
  }

  public static class ST_CoveredBy extends ScalarFunction {

    /** Constructor for relation checking without duplicate removal */
    public ST_CoveredBy() {}

    @DataTypeHint("Boolean")
    public Boolean eval(
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
            Object o1,
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
            Object o2) {
      Geometry geom1 = (Geometry) o1;
      Geometry geom2 = (Geometry) o2;
      return org.apache.sedona.common.Predicates.coveredBy(geom1, geom2);
    }
  }

  public static class ST_Crosses extends ScalarFunction {
    /** Constructor for relation checking without duplicate removal */
    public ST_Crosses() {}

    @DataTypeHint("Boolean")
    public Boolean eval(
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
            Object o1,
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
            Object o2) {
      Geometry geom1 = (Geometry) o1;
      Geometry geom2 = (Geometry) o2;
      return org.apache.sedona.common.Predicates.crosses(geom1, geom2);
    }
  }

  public static class ST_Disjoint extends ScalarFunction {

    /** Constructor for relation checking without duplicate removal */
    public ST_Disjoint() {}

    @DataTypeHint("Boolean")
    public Boolean eval(
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
            Object o1,
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
            Object o2) {
      Geometry geom1 = (Geometry) o1;
      Geometry geom2 = (Geometry) o2;
      return org.apache.sedona.common.Predicates.disjoint(geom1, geom2);
    }
  }

  public static class ST_Equals extends ScalarFunction {

    /** Constructor for relation checking without duplicate removal */
    public ST_Equals() {}

    @DataTypeHint("Boolean")
    public Boolean eval(
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
            Object o1,
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
            Object o2) {
      Geometry geom1 = (Geometry) o1;
      Geometry geom2 = (Geometry) o2;
      return org.apache.sedona.common.Predicates.equals(geom1, geom2);
    }
  }

  public static class ST_OrderingEquals extends ScalarFunction {

    /** Constructor for relation checking without duplicate removal */
    public ST_OrderingEquals() {}

    @DataTypeHint("Boolean")
    public Boolean eval(
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
            Object o1,
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
            Object o2) {
      Geometry geom1 = (Geometry) o1;
      Geometry geom2 = (Geometry) o2;
      return org.apache.sedona.common.Predicates.orderingEquals(geom1, geom2);
    }
  }

  public static class ST_Overlaps extends ScalarFunction {

    /** Constructor for relation checking without duplicate removal */
    public ST_Overlaps() {}

    @DataTypeHint("Boolean")
    public Boolean eval(
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
            Object o1,
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
            Object o2) {
      Geometry geom1 = (Geometry) o1;
      Geometry geom2 = (Geometry) o2;
      return org.apache.sedona.common.Predicates.overlaps(geom1, geom2);
    }
  }

  public static class ST_Relate extends ScalarFunction {
    /** Constructor for relation checking without duplicate removal */
    public ST_Relate() {}

    @DataTypeHint("String")
    public String eval(
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
            Object o1,
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
            Object o2) {
      Geometry geom1 = (Geometry) o1;
      Geometry geom2 = (Geometry) o2;
      return org.apache.sedona.common.Predicates.relate(geom1, geom2);
    }

    @DataTypeHint("Boolean")
    public Boolean eval(
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
            Object o1,
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
            Object o2,
        @DataTypeHint("String") String IM) {
      Geometry geom1 = (Geometry) o1;
      Geometry geom2 = (Geometry) o2;
      return org.apache.sedona.common.Predicates.relate(geom1, geom2, IM);
    }
  }

  public static class ST_RelateMatch extends ScalarFunction {
    /** Constructor for relation checking without duplicate removal */
    public ST_RelateMatch() {}

    @DataTypeHint("Boolean")
    public Boolean eval(
        @DataTypeHint("String") String matrix1, @DataTypeHint("String") String matrix2) {
      return org.apache.sedona.common.Predicates.relateMatch(matrix1, matrix2);
    }
  }

  public static class ST_Touches extends ScalarFunction {

    /** Constructor for relation checking without duplicate removal */
    public ST_Touches() {}

    @DataTypeHint("Boolean")
    public Boolean eval(
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
            Object o1,
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
            Object o2) {
      Geometry geom1 = (Geometry) o1;
      Geometry geom2 = (Geometry) o2;
      return org.apache.sedona.common.Predicates.touches(geom1, geom2);
    }
  }

  public static class ST_DWithin extends ScalarFunction {

    public ST_DWithin() {}

    @DataTypeHint("Boolean")
    public Boolean eval(
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
            Object o1,
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
            Object o2,
        @DataTypeHint("Double") Double distance) {
      Geometry geom1 = (Geometry) o1;
      Geometry geom2 = (Geometry) o2;
      return org.apache.sedona.common.Predicates.dWithin(geom1, geom2, distance);
    }

    @DataTypeHint("Boolean")
    public Boolean eval(
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
            Object o1,
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
            Object o2,
        @DataTypeHint("Double") Double distance,
        @DataTypeHint("Boolean") Boolean useSphere) {
      Geometry geom1 = (Geometry) o1;
      Geometry geom2 = (Geometry) o2;
      return org.apache.sedona.common.Predicates.dWithin(geom1, geom2, distance, useSphere);
    }
  }
}
