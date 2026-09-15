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

import java.io.Serializable;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateSequenceFactory;
import org.locationtech.jts.geom.impl.CoordinateArraySequenceFactory;

/** Preserves binary layout declarations on copies without marking ordinary JTS allocations. */
final class DeclaredCoordinateSequenceFactory implements CoordinateSequenceFactory, Serializable {
  private static final long serialVersionUID = 1L;
  static final DeclaredCoordinateSequenceFactory INSTANCE = new DeclaredCoordinateSequenceFactory();

  private DeclaredCoordinateSequenceFactory() {}

  @Override
  public CoordinateSequence create(Coordinate[] coordinates) {
    // Ordinary Coordinate values may contain a padded NaN Z, which is not a declaration.
    return CoordinateArraySequenceFactory.instance().create(coordinates);
  }

  @Override
  public CoordinateSequence create(CoordinateSequence sequence) {
    if (sequence instanceof DeclaredCoordinateSequence) {
      return sequence.copy();
    }
    return CoordinateArraySequenceFactory.instance().create(sequence);
  }

  @Override
  public CoordinateSequence create(int size, int dimension) {
    return CoordinateArraySequenceFactory.instance().create(size, dimension);
  }

  @Override
  public CoordinateSequence create(int size, int dimension, int measures) {
    return CoordinateArraySequenceFactory.instance().create(size, dimension, measures);
  }
}
