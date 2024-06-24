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
import org.apache.sedona.common.utils.InscribedCircle;
import org.apache.sedona.snowflake.snowsql.GeometrySerde;
import org.apache.sedona.snowflake.snowsql.annotations.UDTFAnnotations;
import org.locationtech.jts.io.ParseException;

@UDTFAnnotations.TabularFunc(
    name = "ST_MaximumInscribedCircle",
    argNames = {"geometry"})
public class ST_MaximumInscribedCircle {

  public static class OutputRow {
    public final byte[] center;
    public final byte[] nearest;
    public final double radius;

    public OutputRow(InscribedCircle inscribedCircle) {
      this.center = GeometrySerde.serialize(inscribedCircle.center);
      this.nearest = GeometrySerde.serialize(inscribedCircle.nearest);
      this.radius = inscribedCircle.radius;
    }
  }

  public static Class getOutputClass() {
    return OutputRow.class;
  }

  public ST_MaximumInscribedCircle() {}

  public Stream<OutputRow> process(byte[] geometry) throws ParseException {
    InscribedCircle inscribedCircle =
        Functions.maximumInscribedCircle(GeometrySerde.deserialize(geometry));

    return Stream.of(new OutputRow(inscribedCircle));
  }
}
