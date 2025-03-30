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
package org.apache.sedona.flink.confluent.constructors;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.sedona.flink.confluent.GeometrySerde;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;

public class ST_PolygonFromEnvelope extends ScalarFunction {

  @DataTypeHint("Bytes")
  public byte[] eval(
      @DataTypeHint("Double") Double minX,
      @DataTypeHint("Double") Double minY,
      @DataTypeHint("Double") Double maxX,
      @DataTypeHint("Double") Double maxY) {
    Coordinate[] coordinates = new Coordinate[5];
    coordinates[0] = new Coordinate(minX, minY);
    coordinates[1] = new Coordinate(minX, maxY);
    coordinates[2] = new Coordinate(maxX, maxY);
    coordinates[3] = new Coordinate(maxX, minY);
    coordinates[4] = coordinates[0];
    GeometryFactory geometryFactory = new GeometryFactory();
    return GeometrySerde.serialize(geometryFactory.createPolygon(coordinates));
  }
}
