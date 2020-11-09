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

package org.apache.sedona.core.geometryObjects;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.log4j.Logger;
import org.apache.sedona.core.formatMapper.shapefileParser.parseUtils.shp.ShapeSerde;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

/**
 * Provides methods to efficiently serialize and deserialize geometry types.
 * <p>
 * Supports Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon,
 * GeometryCollection, Circle and Envelope types.
 * <p>
 * First byte contains {@link Type#id}. Then go type-specific bytes, followed
 * by user-data attached to the geometry.
 */
public class GeometrySerde
        extends Serializer
{

    private static final Logger log = Logger.getLogger(GeometrySerde.class);
    private static final GeometryFactory geometryFactory = new GeometryFactory();

    @Override
    public void write(Kryo kryo, Output out, Object object)
    {
        if (object instanceof Circle) {
            Circle circle = (Circle) object;
            writeType(out, Type.CIRCLE);
            out.writeDouble(circle.getRadius());
            writeGeometry(kryo, out, circle.getCenterGeometry());
            writeUserData(kryo, out, circle);
        }
        else if (object instanceof Point || object instanceof LineString
                || object instanceof Polygon || object instanceof MultiPoint
                || object instanceof MultiLineString || object instanceof MultiPolygon) {
            writeType(out, Type.SHAPE);
            writeGeometry(kryo, out, (Geometry) object);
        }
        else if (object instanceof GeometryCollection) {
            GeometryCollection collection = (GeometryCollection) object;
            writeType(out, Type.GEOMETRYCOLLECTION);
            out.writeInt(collection.getNumGeometries());
            for (int i = 0; i < collection.getNumGeometries(); i++) {
                writeGeometry(kryo, out, collection.getGeometryN(i));
            }
            writeUserData(kryo, out, collection);
        }
        else if (object instanceof Envelope) {
            Envelope envelope = (Envelope) object;
            writeType(out, Type.ENVELOPE);
            out.writeDouble(envelope.getMinX());
            out.writeDouble(envelope.getMaxX());
            out.writeDouble(envelope.getMinY());
            out.writeDouble(envelope.getMaxY());
        }
        else {
            throw new UnsupportedOperationException("Cannot serialize object of type " +
                    object.getClass().getName());
        }
    }

    private void writeType(Output out, Type type)
    {
        out.writeByte((byte) type.id);
    }

    private void writeGeometry(Kryo kryo, Output out, Geometry geometry)
    {
        byte[] data = ShapeSerde.serialize(geometry);
        out.write(data, 0, data.length);
        writeUserData(kryo, out, geometry);
    }

    private void writeUserData(Kryo kryo, Output out, Geometry geometry)
    {
        out.writeBoolean(geometry.getUserData() != null);
        if (geometry.getUserData() != null) {
            kryo.writeClass(out, geometry.getUserData().getClass());
            kryo.writeObject(out, geometry.getUserData());
        }
    }

    @Override
    public Object read(Kryo kryo, Input input, Class aClass)
    {
        byte typeId = input.readByte();
        Type geometryType = Type.fromId(typeId);
        switch (geometryType) {
            case SHAPE:
                return readGeometry(kryo, input);
            case CIRCLE: {
                double radius = input.readDouble();
                Geometry centerGeometry = readGeometry(kryo, input);
                Object userData = readUserData(kryo, input);

                Circle circle = new Circle(centerGeometry, radius);
                circle.setUserData(userData);
                return circle;
            }
            case GEOMETRYCOLLECTION: {
                int numGeometries = input.readInt();
                Geometry[] geometries = new Geometry[numGeometries];
                for (int i = 0; i < numGeometries; i++) {
                    geometries[i] = readGeometry(kryo, input);
                }
                GeometryCollection collection = geometryFactory.createGeometryCollection(geometries);
                collection.setUserData(readUserData(kryo, input));
                return collection;
            }
            case ENVELOPE: {
                double xMin = input.readDouble();
                double xMax = input.readDouble();
                double yMin = input.readDouble();
                double yMax = input.readDouble();
                return new Envelope(xMin, xMax, yMin, yMax);
            }
            default:
                throw new UnsupportedOperationException(
                        "Cannot deserialize object of type " + geometryType);
        }
    }

    private Object readUserData(Kryo kryo, Input input)
    {
        Object userData = null;
        if (input.readBoolean()) {
            Registration clazz = kryo.readClass(input);
            userData = kryo.readObject(input, clazz.getType());
        }
        return userData;
    }

    private Geometry readGeometry(Kryo kryo, Input input)
    {
        Geometry geometry = ShapeSerde.deserialize(input, geometryFactory);
        geometry.setUserData(readUserData(kryo, input));
        return geometry;
    }

    private enum Type
    {
        SHAPE(0),
        CIRCLE(1),
        GEOMETRYCOLLECTION(2),
        ENVELOPE(3);

        private final int id;

        Type(int id)
        {
            this.id = id;
        }

        public static Type fromId(int id)
        {
            for (Type type : values()) {
                if (type.id == id) {
                    return type;
                }
            }

            return null;
        }
    }
}
