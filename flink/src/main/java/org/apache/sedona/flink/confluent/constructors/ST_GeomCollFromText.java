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
import org.locationtech.jts.io.ParseException;

public class ST_GeomCollFromText extends ScalarFunction {

  @DataTypeHint("Bytes")
  public byte[] eval(@DataTypeHint(value = "String") String wkt, @DataTypeHint("Int") Integer srid)
      throws ParseException {
    return GeometrySerde.serialize(
        org.apache.sedona.common.Constructors.geomCollFromText(wkt, srid));
  }

  @DataTypeHint("Bytes")
  public byte[] eval(@DataTypeHint(value = "String") String wkt) throws ParseException {
    return GeometrySerde.serialize(org.apache.sedona.common.Constructors.geomCollFromText(wkt, 0));
  }
}
