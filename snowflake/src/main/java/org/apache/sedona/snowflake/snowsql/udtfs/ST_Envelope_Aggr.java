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
package org.apache.sedona.snowflake.snowsql.udtfs;

import java.util.stream.Stream;
import org.apache.sedona.snowflake.snowsql.GeometrySerde;
import org.apache.sedona.snowflake.snowsql.annotations.UDTFAnnotations;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;

@UDTFAnnotations.TabularFunc(
    name = "ST_Envelope_Aggr",
    argNames = {"geom"})
public class ST_Envelope_Aggr {

  public static final GeometryFactory geometryFactory = new GeometryFactory();

  Envelope buffer = null;

  public static class OutputRow {

    public byte[] envelope;

    public OutputRow(byte[] envelopePolygon) {
      this.envelope = envelopePolygon;
    }
  }

  public static Class getOutputClass() {
    return OutputRow.class;
  }

  public ST_Envelope_Aggr() {}

  public Stream<OutputRow> process(byte[] geom) throws ParseException {
    Geometry geometry = GeometrySerde.deserialize(geom);
    if (buffer == null) {
      buffer = geometry.getEnvelopeInternal();
    } else {
      buffer.expandToInclude(geometry.getEnvelopeInternal());
    }
    return Stream.empty();
  }

  public Stream<OutputRow> endPartition() {
    // Returns the value we initialized in the constructor.
    Polygon poly =
        geometryFactory.createPolygon(
            geometryFactory.createLinearRing(
                new Coordinate[] {
                  new Coordinate(buffer.getMinX(), buffer.getMinY()),
                  new Coordinate(buffer.getMinX(), buffer.getMaxY()),
                  new Coordinate(buffer.getMaxX(), buffer.getMaxY()),
                  new Coordinate(buffer.getMaxX(), buffer.getMinY()),
                  new Coordinate(buffer.getMinX(), buffer.getMinY())
                }));
    return Stream.of(new OutputRow(GeometrySerde.serialize(poly)));
  }
}
