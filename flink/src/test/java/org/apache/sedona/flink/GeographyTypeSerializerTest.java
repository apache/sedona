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
package org.apache.sedona.flink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.sedona.common.S2Geography.Geography;
import org.apache.sedona.common.geography.Constructors;
import org.junit.Test;

public class GeographyTypeSerializerTest {

  private final GeographyTypeSerializer serializer = GeographyTypeSerializer.INSTANCE;

  private Geography roundTrip(Geography input) throws Exception {
    DataOutputSerializer out = new DataOutputSerializer(64);
    serializer.serialize(input, out);
    DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
    return serializer.deserialize(in);
  }

  @Test
  public void testPointRoundTrip() throws Exception {
    Geography point = Constructors.geogFromWKT("POINT (1 2)", 4326);
    Geography result = roundTrip(point);
    assertEquals(point.toEWKT(), result.toEWKT());
    assertEquals(4326, result.getSRID());
  }

  @Test
  public void testPolygonRoundTrip() throws Exception {
    Geography polygon = Constructors.geogFromWKT("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", 4326);
    Geography result = roundTrip(polygon);
    assertEquals(polygon.toEWKT(), result.toEWKT());
  }

  @Test
  public void testNullRoundTrip() throws Exception {
    assertNull(roundTrip(null));
  }

  @Test
  public void testCopy() throws Exception {
    Geography point = Constructors.geogFromWKT("POINT (3 4)", 4326);
    Geography copy = serializer.copy(point);
    assertEquals(point.toEWKT(), copy.toEWKT());
    assertNull(serializer.copy(null));
  }
}
