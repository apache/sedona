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

package org.apache.sedona.core.formatMapper.shapefileParser.shapes;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.sedona.core.geometryObjects.Circle;
import org.apache.sedona.core.geometryObjects.GeometrySerde;
import org.apache.sedona.core.utils.GeomUtils;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.io.ByteArrayOutputStream;

import static org.junit.Assert.assertTrue;

public class GeometrySerdeTest
{
    private final Kryo kryo = new Kryo();
    private final WKTReader wktReader = new WKTReader();

    @Test
    public void test()
            throws Exception
    {
        test("POINT (1.3 4.5)");
        test("MULTIPOINT ((1 1), (1.3 4.5), (5.2 999))");
        test("LINESTRING (1 1, 1.3 4.5, 5.2 999)");
        test("MULTILINESTRING ((1 1, 1.3 4.5, 5.2 999), (0 0, 0 1))");
        test("POLYGON ((0 0, 0 1, 1 1, 1 0.4, 0 0))");
        test("POLYGON ((0 0, 0 1, 1 1, 1 0.4, 0 0), (0.2 0.2, 0.5 0.2, 0.5 0.5, 0.2 0.5, 0.2 0.2))");
        test("MULTIPOLYGON (((0 0, 0 1, 1 1, 1 0.4, 0 0)), " +
                "((0 0, 0 1, 1 1, 1 0.4, 0 0), (0.2 0.2, 0.5 0.2, 0.5 0.5, 0.2 0.5, 0.2 0.2)))");
        test("GEOMETRYCOLLECTION (POINT(4 6), LINESTRING(4 6,7 10))");
    }

    private void test(String wkt)
            throws Exception
    {
        Geometry geometry = parseWkt(wkt);
        assertTrue(GeomUtils.equalsExactGeom(geometry, serde(geometry)));

        geometry.setUserData("This is a test");
        assertTrue(GeomUtils.equalsExactGeom(geometry, serde(geometry)));

        if (geometry instanceof GeometryCollection) {
            return;
        }

        Circle circle = new Circle(geometry, 1.2);
        assertTrue(GeomUtils.equalsExactGeom(circle, serde(circle)));
    }

    private Geometry parseWkt(String wkt)
            throws ParseException
    {
        return wktReader.read(wkt);
    }

    private Geometry serde(Geometry input)
    {
        byte[] ser = serialize(input);
        return kryo.readObject(new Input(ser), input.getClass());
    }

    private byte[] serialize(Geometry input)
    {
        kryo.register(input.getClass(), new GeometrySerde());

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        Output output = new Output(bos);
        kryo.writeObject(output, input);
        output.close();
        return bos.toByteArray();
    }
}
