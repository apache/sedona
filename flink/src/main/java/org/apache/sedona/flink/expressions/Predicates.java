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
import org.apache.sedona.common.geometryObjects.Box2D;
import org.apache.sedona.common.geometryObjects.Box3D;
import org.apache.sedona.flink.Box2DTypeSerializer;
import org.apache.sedona.flink.Box3DTypeSerializer;
import org.apache.sedona.flink.GeographyTypeSerializer;
import org.apache.sedona.flink.GeometryTypeSerializer;
import org.locationtech.jts.geom.Geometry;

public class Predicates {

  public static class ST_Intersects extends ScalarFunction {
    /** Constructor for relation checking without duplicate removal */
    public ST_Intersects() {}

    @DataTypeHint("Boolean")
    public Boolean eval(
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
            Object o1,
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
            Object o2) {
      if (o1 == null || o2 == null) return null;
      Geometry geom1 = (Geometry) o1;
      Geometry geom2 = (Geometry) o2;
      return org.apache.sedona.common.Predicates.intersects(geom1, geom2);
    }

    /** Box2D-on-Box2D bbox intersection (closed interval). */
    @DataTypeHint("Boolean")
    public Boolean eval(
        @DataTypeHint(
                value = "RAW",
                rawSerializer = Box2DTypeSerializer.class,
                bridgedTo = Box2D.class)
            Box2D a,
        @DataTypeHint(
                value = "RAW",
                rawSerializer = Box2DTypeSerializer.class,
                bridgedTo = Box2D.class)
            Box2D b) {
      if (a == null || b == null) return null;
      return org.apache.sedona.common.Predicates.boxIntersects(a, b);
    }

    /** Box3D-on-Box3D bbox intersection on all three axes (closed interval). */
    @DataTypeHint("Boolean")
    public Boolean eval(
        @DataTypeHint(
                value = "RAW",
                rawSerializer = Box3DTypeSerializer.class,
                bridgedTo = Box3D.class)
            Box3D a,
        @DataTypeHint(
                value = "RAW",
                rawSerializer = Box3DTypeSerializer.class,
                bridgedTo = Box3D.class)
            Box3D b) {
      if (a == null || b == null) return null;
      return org.apache.sedona.common.Predicates.box3dIntersects(a, b);
    }

    @DataTypeHint("Boolean")
    public Boolean eval(
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeographyTypeSerializer.class,
                bridgedTo = org.apache.sedona.common.S2Geography.Geography.class)
            org.apache.sedona.common.S2Geography.Geography g1,
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeographyTypeSerializer.class,
                bridgedTo = org.apache.sedona.common.S2Geography.Geography.class)
            org.apache.sedona.common.S2Geography.Geography g2) {
      if (g1 == null || g2 == null) return null;
      return org.apache.sedona.common.geography.Functions.intersects(g1, g2);
    }
  }

  public static class ST_Contains extends ScalarFunction {

    /** Constructor for relation checking without duplicate removal */
    public ST_Contains() {}

    @DataTypeHint("Boolean")
    public Boolean eval(
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
            Object o1,
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
            Object o2) {
      if (o1 == null || o2 == null) return null;
      Geometry geom1 = (Geometry) o1;
      Geometry geom2 = (Geometry) o2;
      return org.apache.sedona.common.Predicates.contains(geom1, geom2);
    }

    /** Box2D-on-Box2D bbox containment (closed interval). */
    @DataTypeHint("Boolean")
    public Boolean eval(
        @DataTypeHint(
                value = "RAW",
                rawSerializer = Box2DTypeSerializer.class,
                bridgedTo = Box2D.class)
            Box2D a,
        @DataTypeHint(
                value = "RAW",
                rawSerializer = Box2DTypeSerializer.class,
                bridgedTo = Box2D.class)
            Box2D b) {
      if (a == null || b == null) return null;
      return org.apache.sedona.common.Predicates.boxContains(a, b);
    }

    /** Box3D-on-Box3D bbox containment on all three axes (closed interval). */
    @DataTypeHint("Boolean")
    public Boolean eval(
        @DataTypeHint(
                value = "RAW",
                rawSerializer = Box3DTypeSerializer.class,
                bridgedTo = Box3D.class)
            Box3D a,
        @DataTypeHint(
                value = "RAW",
                rawSerializer = Box3DTypeSerializer.class,
                bridgedTo = Box3D.class)
            Box3D b) {
      if (a == null || b == null) return null;
      return org.apache.sedona.common.Predicates.box3dContains(a, b);
    }

    @DataTypeHint("Boolean")
    public Boolean eval(
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeographyTypeSerializer.class,
                bridgedTo = org.apache.sedona.common.S2Geography.Geography.class)
            org.apache.sedona.common.S2Geography.Geography g1,
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeographyTypeSerializer.class,
                bridgedTo = org.apache.sedona.common.S2Geography.Geography.class)
            org.apache.sedona.common.S2Geography.Geography g2) {
      if (g1 == null || g2 == null) return null;
      return org.apache.sedona.common.geography.Functions.contains(g1, g2);
    }
  }

  public static class ST_Within extends ScalarFunction {
    /** Constructor for relation checking without duplicate removal */
    public ST_Within() {}

    @DataTypeHint("Boolean")
    public Boolean eval(
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
            Object o1,
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
            Object o2) {
      Geometry geom1 = (Geometry) o1;
      Geometry geom2 = (Geometry) o2;
      return org.apache.sedona.common.Predicates.within(geom1, geom2);
    }

    @DataTypeHint("Boolean")
    public Boolean eval(
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeographyTypeSerializer.class,
                bridgedTo = org.apache.sedona.common.S2Geography.Geography.class)
            org.apache.sedona.common.S2Geography.Geography g1,
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeographyTypeSerializer.class,
                bridgedTo = org.apache.sedona.common.S2Geography.Geography.class)
            org.apache.sedona.common.S2Geography.Geography g2) {
      if (g1 == null || g2 == null) return null;
      return org.apache.sedona.common.geography.Functions.within(g1, g2);
    }
  }

  public static class ST_Covers extends ScalarFunction {

    /** Constructor for relation checking without duplicate removal */
    public ST_Covers() {}

    @DataTypeHint("Boolean")
    public Boolean eval(
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
            Object o1,
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
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
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
            Object o1,
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
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
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
            Object o1,
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
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
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
            Object o1,
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
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
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
            Object o1,
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
            Object o2) {
      Geometry geom1 = (Geometry) o1;
      Geometry geom2 = (Geometry) o2;
      return org.apache.sedona.common.Predicates.equals(geom1, geom2);
    }

    @DataTypeHint("Boolean")
    public Boolean eval(
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeographyTypeSerializer.class,
                bridgedTo = org.apache.sedona.common.S2Geography.Geography.class)
            org.apache.sedona.common.S2Geography.Geography g1,
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeographyTypeSerializer.class,
                bridgedTo = org.apache.sedona.common.S2Geography.Geography.class)
            org.apache.sedona.common.S2Geography.Geography g2) {
      if (g1 == null || g2 == null) return null;
      return org.apache.sedona.common.geography.Functions.equals(g1, g2);
    }
  }

  public static class ST_OrderingEquals extends ScalarFunction {

    /** Constructor for relation checking without duplicate removal */
    public ST_OrderingEquals() {}

    @DataTypeHint("Boolean")
    public Boolean eval(
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
            Object o1,
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
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
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
            Object o1,
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
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
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
            Object o1,
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
            Object o2) {
      Geometry geom1 = (Geometry) o1;
      Geometry geom2 = (Geometry) o2;
      return org.apache.sedona.common.Predicates.relate(geom1, geom2);
    }

    @DataTypeHint("Boolean")
    public Boolean eval(
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
            Object o1,
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
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
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
            Object o1,
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
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
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
            Object o1,
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
            Object o2,
        @DataTypeHint("Double") Double distance) {
      // The common-layer dWithin takes a primitive double, so a SQL NULL distance would
      // autounbox to an NPE; any NULL argument propagates to a NULL result instead.
      if (o1 == null || o2 == null || distance == null) return null;
      Geometry geom1 = (Geometry) o1;
      Geometry geom2 = (Geometry) o2;
      return org.apache.sedona.common.Predicates.dWithin(geom1, geom2, distance);
    }

    @DataTypeHint("Boolean")
    public Boolean eval(
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
            Object o1,
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
            Object o2,
        @DataTypeHint("Double") Double distance,
        @DataTypeHint("Boolean") Boolean useSphere) {
      // distance and useSphere both autounbox to primitives in the common layer; guard all four.
      if (o1 == null || o2 == null || distance == null || useSphere == null) return null;
      Geometry geom1 = (Geometry) o1;
      Geometry geom2 = (Geometry) o2;
      return org.apache.sedona.common.Predicates.dWithin(geom1, geom2, distance, useSphere);
    }

    @DataTypeHint("Boolean")
    public Boolean eval(
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeographyTypeSerializer.class,
                bridgedTo = org.apache.sedona.common.S2Geography.Geography.class)
            org.apache.sedona.common.S2Geography.Geography g1,
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeographyTypeSerializer.class,
                bridgedTo = org.apache.sedona.common.S2Geography.Geography.class)
            org.apache.sedona.common.S2Geography.Geography g2,
        @DataTypeHint("Double") Double distance) {
      if (g1 == null || g2 == null || distance == null) return null;
      return org.apache.sedona.common.geography.Functions.dWithin(g1, g2, distance);
    }
  }

  public static class ST_3DDWithin extends ScalarFunction {

    public ST_3DDWithin() {}

    /** 3D Euclidean distance-within over two geometries (missing Z folds to 0). */
    @DataTypeHint("Boolean")
    public Boolean eval(
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
            Object o1,
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
            Object o2,
        @DataTypeHint("Double") Double distance) {
      // Guard distance too: the common-layer dWithin3D takes a primitive double, so a SQL NULL
      // distance would autounbox to an NPE rather than propagating NULL.
      if (o1 == null || o2 == null || distance == null) return null;
      Geometry geom1 = (Geometry) o1;
      Geometry geom2 = (Geometry) o2;
      return org.apache.sedona.common.Predicates.dWithin3D(geom1, geom2, distance);
    }

    /** Closed-interval 3D distance test over two Box3D values. */
    @DataTypeHint("Boolean")
    public Boolean eval(
        @DataTypeHint(
                value = "RAW",
                rawSerializer = Box3DTypeSerializer.class,
                bridgedTo = Box3D.class)
            Box3D a,
        @DataTypeHint(
                value = "RAW",
                rawSerializer = Box3DTypeSerializer.class,
                bridgedTo = Box3D.class)
            Box3D b,
        @DataTypeHint("Double") Double distance) {
      if (a == null || b == null || distance == null) return null;
      return org.apache.sedona.common.Predicates.dWithin3D(a, b, distance);
    }
  }
}
