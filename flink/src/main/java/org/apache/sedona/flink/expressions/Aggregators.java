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
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.sedona.common.geometryObjects.Box2D;
import org.apache.sedona.common.geometryObjects.Box3D;
import org.apache.sedona.flink.Box2DTypeSerializer;
import org.apache.sedona.flink.Box3DTypeSerializer;
import org.apache.sedona.flink.GeometryTypeSerializer;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

public class Aggregators {
  // Compute the rectangular boundary of a number of geometries
  @DataTypeHint(
      value = "RAW",
      rawSerializer = GeometryTypeSerializer.class,
      bridgedTo = Geometry.class)
  public static class ST_Envelope_Aggr extends AggregateFunction<Geometry, Accumulators.Envelope> {

    Geometry createPolygon(double minX, double minY, double maxX, double maxY) {
      Coordinate[] coords = new Coordinate[5];
      coords[0] = new Coordinate(minX, minY);
      coords[1] = new Coordinate(minX, maxY);
      coords[2] = new Coordinate(maxX, maxY);
      coords[3] = new Coordinate(maxX, minY);
      coords[4] = coords[0];
      GeometryFactory geomFact = new GeometryFactory();
      return geomFact.createPolygon(coords);
    }

    @Override
    public Accumulators.Envelope createAccumulator() {
      return new Accumulators.Envelope();
    }

    @Override
    @DataTypeHint(
        value = "RAW",
        rawSerializer = GeometryTypeSerializer.class,
        bridgedTo = Geometry.class)
    public Geometry getValue(Accumulators.Envelope acc) {
      if (acc.minX > acc.maxX) return null;
      return createPolygon(acc.minX, acc.minY, acc.maxX, acc.maxY);
    }

    public void accumulate(
        Accumulators.Envelope acc,
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
            Object o) {
      if (o == null) return;
      Geometry geometry = (Geometry) o;
      if (geometry.isEmpty()) return;
      Envelope envelope = geometry.getEnvelopeInternal();
      acc.minX = Math.min(acc.minX, envelope.getMinX());
      acc.minY = Math.min(acc.minY, envelope.getMinY());
      acc.maxX = Math.max(acc.maxX, envelope.getMaxX());
      acc.maxY = Math.max(acc.maxY, envelope.getMaxY());
    }

    /**
     * TODO: find an efficient algorithm to incrementally and decrementally update the accumulator
     *
     * @param acc
     * @param o
     */
    public void retract(
        Accumulators.Envelope acc,
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
            Object o) {
      Geometry geometry = (Geometry) o;
      assert (false);
    }

    public void merge(Accumulators.Envelope acc, Iterable<Accumulators.Envelope> it) {
      for (Accumulators.Envelope a : it) {
        acc.minX = Math.min(acc.minX, a.minX);
        acc.minY = Math.min(acc.minY, a.minY);
        acc.maxX = Math.max(acc.maxX, a.maxX);
        acc.maxY = Math.max(acc.maxY, a.maxY);
      }
    }

    public void resetAccumulator(Accumulators.Envelope acc) {
      acc.reset();
    }
  }

  // Aggregate the bounding box of all input geometries as a Box2D. Mirrors PostGIS ST_Extent.
  // Returns null when there are no rows or all inputs are null/empty.
  @DataTypeHint(value = "RAW", rawSerializer = Box2DTypeSerializer.class, bridgedTo = Box2D.class)
  public static class ST_Extent extends AggregateFunction<Box2D, Accumulators.Envelope> {

    @Override
    public Accumulators.Envelope createAccumulator() {
      return new Accumulators.Envelope();
    }

    @Override
    @DataTypeHint(value = "RAW", rawSerializer = Box2DTypeSerializer.class, bridgedTo = Box2D.class)
    public Box2D getValue(Accumulators.Envelope acc) {
      if (acc.minX > acc.maxX) return null;
      return new Box2D(acc.minX, acc.minY, acc.maxX, acc.maxY);
    }

    public void accumulate(
        Accumulators.Envelope acc,
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
            Object o) {
      if (o == null) return;
      Geometry geometry = (Geometry) o;
      if (geometry.isEmpty()) return;
      Envelope envelope = geometry.getEnvelopeInternal();
      acc.minX = Math.min(acc.minX, envelope.getMinX());
      acc.minY = Math.min(acc.minY, envelope.getMinY());
      acc.maxX = Math.max(acc.maxX, envelope.getMaxX());
      acc.maxY = Math.max(acc.maxY, envelope.getMaxY());
    }

    public void retract(
        Accumulators.Envelope acc,
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
            Object o) {
      assert (false);
    }

    public void merge(Accumulators.Envelope acc, Iterable<Accumulators.Envelope> it) {
      for (Accumulators.Envelope a : it) {
        acc.minX = Math.min(acc.minX, a.minX);
        acc.minY = Math.min(acc.minY, a.minY);
        acc.maxX = Math.max(acc.maxX, a.maxX);
        acc.maxY = Math.max(acc.maxY, a.maxY);
      }
    }

    public void resetAccumulator(Accumulators.Envelope acc) {
      acc.reset();
    }
  }

  // Aggregate the 3D bounding box of all input geometries as a Box3D. Mirrors PostGIS
  // ST_3DExtent. Geometries without a Z dimension fold into z = 0 per coordinate (via
  // Functions.box3D). Returns null when there are no rows or all inputs are null/empty.
  @DataTypeHint(value = "RAW", rawSerializer = Box3DTypeSerializer.class, bridgedTo = Box3D.class)
  public static class ST_3DExtent extends AggregateFunction<Box3D, Accumulators.Envelope3D> {

    @Override
    public Accumulators.Envelope3D createAccumulator() {
      return new Accumulators.Envelope3D();
    }

    @Override
    @DataTypeHint(value = "RAW", rawSerializer = Box3DTypeSerializer.class, bridgedTo = Box3D.class)
    public Box3D getValue(Accumulators.Envelope3D acc) {
      if (acc.minX > acc.maxX) return null;
      return new Box3D(acc.minX, acc.minY, acc.minZ, acc.maxX, acc.maxY, acc.maxZ);
    }

    public void accumulate(
        Accumulators.Envelope3D acc,
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
            Object o) {
      if (o == null) return;
      // Functions.box3D folds missing Z to 0 and returns null for empty geometries.
      Box3D box = org.apache.sedona.common.Functions.box3D((Geometry) o);
      if (box == null) return;
      acc.minX = Math.min(acc.minX, box.getXMin());
      acc.minY = Math.min(acc.minY, box.getYMin());
      acc.minZ = Math.min(acc.minZ, box.getZMin());
      acc.maxX = Math.max(acc.maxX, box.getXMax());
      acc.maxY = Math.max(acc.maxY, box.getYMax());
      acc.maxZ = Math.max(acc.maxZ, box.getZMax());
    }

    public void retract(
        Accumulators.Envelope3D acc,
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
            Object o) {
      assert (false);
    }

    public void merge(Accumulators.Envelope3D acc, Iterable<Accumulators.Envelope3D> it) {
      for (Accumulators.Envelope3D a : it) {
        acc.minX = Math.min(acc.minX, a.minX);
        acc.minY = Math.min(acc.minY, a.minY);
        acc.minZ = Math.min(acc.minZ, a.minZ);
        acc.maxX = Math.max(acc.maxX, a.maxX);
        acc.maxY = Math.max(acc.maxY, a.maxY);
        acc.maxZ = Math.max(acc.maxZ, a.maxZ);
      }
    }

    public void resetAccumulator(Accumulators.Envelope3D acc) {
      acc.reset();
    }
  }

  // Compute the Union boundary of numbers of geometries
  //
  @DataTypeHint(
      value = "RAW",
      rawSerializer = GeometryTypeSerializer.class,
      bridgedTo = Geometry.class)
  public static class ST_Intersection_Aggr
      extends AggregateFunction<Geometry, Accumulators.AccGeometry> {

    @Override
    public Accumulators.AccGeometry createAccumulator() {
      return new Accumulators.AccGeometry();
    }

    @Override
    @DataTypeHint(
        value = "RAW",
        rawSerializer = GeometryTypeSerializer.class,
        bridgedTo = Geometry.class)
    public Geometry getValue(Accumulators.AccGeometry acc) {
      return acc.geom;
    }

    public void accumulate(
        Accumulators.AccGeometry acc,
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
            Object o) {
      if (acc.geom == null) {
        acc.geom = (Geometry) o;
      } else {
        acc.geom = acc.geom.intersection((Geometry) o);
      }
    }

    /**
     * TODO: find an efficient algorithm to incrementally and decrementally update the accumulator
     *
     * @param acc
     * @param o
     */
    public void retract(
        Accumulators.AccGeometry acc,
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
            Object o) {
      Geometry geometry = (Geometry) o;
      assert (false);
    }

    public void merge(Accumulators.AccGeometry acc, Iterable<Accumulators.AccGeometry> it) {
      for (Accumulators.AccGeometry a : it) {
        if (acc.geom == null) {
          //      make accumulate equal to acc
          acc.geom = a.geom;
        } else {
          acc.geom = acc.geom.intersection(a.geom);
        }
      }
    }

    public void resetAccumulator(Accumulators.AccGeometry acc) {
      acc.geom = null;
    }
  }

  // Compute the Union boundary of numbers of geometries
  //
  @DataTypeHint(
      value = "RAW",
      rawSerializer = GeometryTypeSerializer.class,
      bridgedTo = Geometry.class)
  public static class ST_Union_Aggr extends AggregateFunction<Geometry, Accumulators.AccGeometry> {

    @Override
    public Accumulators.AccGeometry createAccumulator() {
      return new Accumulators.AccGeometry();
    }

    @Override
    @DataTypeHint(
        value = "RAW",
        rawSerializer = GeometryTypeSerializer.class,
        bridgedTo = Geometry.class)
    public Geometry getValue(Accumulators.AccGeometry acc) {
      return acc.geom;
    }

    public void accumulate(
        Accumulators.AccGeometry acc,
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
            Object o) {
      if (acc.geom == null) {
        acc.geom = (Geometry) o;
      } else {
        acc.geom = acc.geom.union((Geometry) o);
      }
    }

    /**
     * TODO: find an efficient algorithm to incrementally and decrementally update the accumulator
     *
     * @param acc
     * @param o
     */
    public void retract(
        Accumulators.AccGeometry acc,
        @DataTypeHint(
                value = "RAW",
                rawSerializer = GeometryTypeSerializer.class,
                bridgedTo = Geometry.class)
            Object o) {
      Geometry geometry = (Geometry) o;
      assert (false);
    }

    public void merge(Accumulators.AccGeometry acc, Iterable<Accumulators.AccGeometry> it) {
      for (Accumulators.AccGeometry a : it) {
        if (acc.geom == null) {
          //      make accumulate equal to acc
          acc.geom = a.geom;
        } else {
          acc.geom = acc.geom.union(a.geom);
        }
      }
    }

    public void resetAccumulator(Accumulators.AccGeometry acc) {
      acc.geom = null;
    }
  }

  // Aliases for *_Aggr functions with *_Agg suffix
  @DataTypeHint(
      value = "RAW",
      rawSerializer = GeometryTypeSerializer.class,
      bridgedTo = Geometry.class)
  public static class ST_Envelope_Agg extends ST_Envelope_Aggr {}

  @DataTypeHint(
      value = "RAW",
      rawSerializer = GeometryTypeSerializer.class,
      bridgedTo = Geometry.class)
  public static class ST_Intersection_Agg extends ST_Intersection_Aggr {}

  @DataTypeHint(
      value = "RAW",
      rawSerializer = GeometryTypeSerializer.class,
      bridgedTo = Geometry.class)
  public static class ST_Union_Agg extends ST_Union_Aggr {}
}
