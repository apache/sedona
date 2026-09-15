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
package org.apache.sedona.common.geometrySerde;

import org.datasyslab.jts.io.WKBReader;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;

/**
 * Reads WKB while retaining explicitly declared coordinate dimensions, including empty geometry.
 */
public final class GeometryWkbReader {
  private GeometryWkbReader() {}

  public static Geometry read(byte[] bytes) throws ParseException {
    return read(bytes, 0);
  }

  public static Geometry read(byte[] bytes, int defaultSrid) throws ParseException {
    return new WKBReader(new ReaderGeometryFactory(defaultSrid)).read(bytes);
  }

  /**
   * WKBReader allocates sequences from this factory using dimensions from binary headers. Geometry
   * construction delegates to a regular factory so that subsequent JTS operations do not mistake
   * sized allocations for binary declarations. This also avoids copying the parsed coordinates or
   * changing SRIDs assigned by WKBReader to individual collection members.
   */
  private static final class ReaderGeometryFactory extends GeometryFactory {
    private static final long serialVersionUID = 1L;
    private final GeometryFactory resultFactory;

    private ReaderGeometryFactory(int srid) {
      super(new PrecisionModel(), srid, new ReaderCoordinateSequenceFactory());
      resultFactory = new DeclaredGeometryFactory(getPrecisionModel(), srid);
    }

    @Override
    public Point createPoint(CoordinateSequence coordinates) {
      return resultFactory.createPoint(coordinates);
    }

    @Override
    public LineString createLineString(CoordinateSequence coordinates) {
      return resultFactory.createLineString(coordinates);
    }

    @Override
    public LinearRing createLinearRing(CoordinateSequence coordinates) {
      return resultFactory.createLinearRing(coordinates);
    }

    @Override
    public Polygon createPolygon(LinearRing shell, LinearRing[] holes) {
      return resultFactory.createPolygon(shell, holes);
    }

    @Override
    public MultiPoint createMultiPoint(Point[] points) {
      return resultFactory.createMultiPoint(points);
    }

    @Override
    public MultiLineString createMultiLineString(LineString[] lines) {
      return resultFactory.createMultiLineString(lines);
    }

    @Override
    public MultiPolygon createMultiPolygon(Polygon[] polygons) {
      return resultFactory.createMultiPolygon(polygons);
    }

    @Override
    public GeometryCollection createGeometryCollection(Geometry[] geometries) {
      return resultFactory.createGeometryCollection(geometries);
    }
  }

  private static final class ReaderCoordinateSequenceFactory implements CoordinateSequenceFactory {
    @Override
    public CoordinateSequence create(Coordinate[] coordinates) {
      return DeclaredCoordinateSequenceFactory.INSTANCE.create(coordinates);
    }

    @Override
    public CoordinateSequence create(CoordinateSequence sequence) {
      return DeclaredCoordinateSequenceFactory.INSTANCE.create(sequence);
    }

    @Override
    public CoordinateSequence create(int size, int dimension) {
      return create(size, dimension, 0);
    }

    @Override
    public CoordinateSequence create(int size, int dimension, int measures) {
      return new DeclaredCoordinateSequence(size, dimension, measures);
    }
  }
}
