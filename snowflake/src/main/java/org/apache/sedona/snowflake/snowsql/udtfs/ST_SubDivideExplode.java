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
import org.apache.sedona.common.Functions;
import org.apache.sedona.snowflake.snowsql.GeometrySerde;
import org.apache.sedona.snowflake.snowsql.annotations.UDTFAnnotations;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;

@UDTFAnnotations.TabularFunc(
    name = "ST_SubDivideExplode",
    argNames = {"geom", "maxVertices"})
public class ST_SubDivideExplode {

  public static final GeometryFactory geometryFactory = new GeometryFactory();

  public static class OutputRow {

    public byte[] geom;

    public OutputRow(byte[] geom) {
      this.geom = geom;
    }
  }

  public static Class getOutputClass() {
    return OutputRow.class;
  }

  public ST_SubDivideExplode() {}

  public Stream<OutputRow> process(byte[] geometry, int maxVertices) throws ParseException {
    Geometry[] geometries = Functions.subDivide(GeometrySerde.deserialize(geometry), maxVertices);
    return Stream.of(geometries).map(g -> new OutputRow(GeometrySerde.serialize(g)));
  }
}
