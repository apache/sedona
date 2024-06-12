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
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

public class Aggregators {
  // Compute the rectangular boundary of a number of geometries
  @DataTypeHint(value = "RAW", bridgedTo = Geometry.class)
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
    @DataTypeHint(value = "RAW", bridgedTo = Geometry.class)
    public Geometry getValue(Accumulators.Envelope acc) {
      return createPolygon(acc.minX, acc.minY, acc.maxX, acc.maxY);
    }

    public void accumulate(
        Accumulators.Envelope acc,
        @DataTypeHint(value = "RAW", bridgedTo = Geometry.class) Object o) {
      Envelope envelope = ((Geometry) o).getEnvelopeInternal();
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
        @DataTypeHint(value = "RAW", bridgedTo = Geometry.class) Object o) {
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

  // Compute the Union boundary of numbers of geometries
  //
  @DataTypeHint(value = "RAW", bridgedTo = Geometry.class)
  public static class ST_Intersection_Aggr
      extends AggregateFunction<Geometry, Accumulators.AccGeometry> {

    @Override
    public Accumulators.AccGeometry createAccumulator() {
      return new Accumulators.AccGeometry();
    }

    @Override
    @DataTypeHint(value = "RAW", bridgedTo = Geometry.class)
    public Geometry getValue(Accumulators.AccGeometry acc) {
      return acc.geom;
    }

    public void accumulate(
        Accumulators.AccGeometry acc,
        @DataTypeHint(value = "RAW", bridgedTo = Geometry.class) Object o) {
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
        @DataTypeHint(value = "RAW", bridgedTo = Geometry.class) Object o) {
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
  @DataTypeHint(value = "RAW", bridgedTo = Geometry.class)
  public static class ST_Union_Aggr extends AggregateFunction<Geometry, Accumulators.AccGeometry> {

    @Override
    public Accumulators.AccGeometry createAccumulator() {
      return new Accumulators.AccGeometry();
    }

    @Override
    @DataTypeHint(value = "RAW", bridgedTo = Geometry.class)
    public Geometry getValue(Accumulators.AccGeometry acc) {
      return acc.geom;
    }

    public void accumulate(
        Accumulators.AccGeometry acc,
        @DataTypeHint(value = "RAW", bridgedTo = Geometry.class) Object o) {
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
        @DataTypeHint(value = "RAW", bridgedTo = Geometry.class) Object o) {
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
}
