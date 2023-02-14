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

import org.junit.Test;
import org.junit.Assert;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;

public class MultiLineStringSerdeTest {
    private static final GeometryFactory gf = new GeometryFactory();

    @Test
    public void testEmptyMultiLineString() {
        MultiLineString multiLineString = gf.createMultiLineString();
        multiLineString.setSRID(4326);
        byte[] bytes = GeometrySerializer.serialize(multiLineString);
        Geometry geom = GeometrySerializer.deserialize(bytes);
        Assert.assertTrue(geom instanceof MultiLineString);
        Assert.assertTrue(geom.isEmpty());
        Assert.assertEquals(4326, geom.getSRID());
    }

    @Test
    public void testMultiLineString() {
        MultiLineString multiLineString =
                gf.createMultiLineString(
                        new LineString[]{
                                gf.createLineString(
                                        new Coordinate[]{
                                                new Coordinate(1, 2), new Coordinate(3, 4), new Coordinate(5, 6),
                                        }),
                                gf.createLineString(),
                                gf.createLineString(
                                        new Coordinate[]{
                                                new Coordinate(7, 8), new Coordinate(9, 10), new Coordinate(11, 12),
                                        }),
                        });
        multiLineString.setSRID(4326);
        byte[] bytes = GeometrySerializer.serialize(multiLineString);
        Geometry geom = GeometrySerializer.deserialize(bytes);
        Assert.assertTrue(geom instanceof MultiLineString);
        Assert.assertEquals(4326, geom.getSRID());
        MultiLineString multiLineString2 = (MultiLineString) geom;
        Assert.assertEquals(3, multiLineString2.getNumGeometries());
        Assert.assertEquals(multiLineString, multiLineString2);
    }

    @Test
    public void testMultiLineStringContainingEmptyLineStrings() {
        MultiLineString multiLineString = gf.createMultiLineString(
                new LineString[] {
                        gf.createLineString(),
                        gf.createLineString(),
                        gf.createLineString()
                }
        );
        multiLineString.setSRID(4326);
        byte[] bytes = GeometrySerializer.serialize(multiLineString);
        Geometry geom = GeometrySerializer.deserialize(bytes);
        Assert.assertEquals(3, geom.getNumGeometries());
        Assert.assertEquals(multiLineString, geom);
    }
}
