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

import org.apache.sedona.common.geometryObjects.StructurePreservingGeometryFactory;
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
    GeometryFactory factory =
        new StructurePreservingGeometryFactory(
            new PrecisionModel(), defaultSrid, DeclaredCoordinateSequenceFactory.INSTANCE);
    // Only allocations made from WKB headers declare a layout. Later JTS operations use the
    // geometry factory's ordinary allocation behavior.
    return new WKBReader(factory, new ReaderCoordinateSequenceFactory()).read(bytes);
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
