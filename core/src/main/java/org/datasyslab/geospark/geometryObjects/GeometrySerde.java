/*
 * FILE: GeometrySerde
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package org.datasyslab.geospark.geometryObjects;

import org.apache.log4j.Logger;
import org.datasyslab.geospark.SpatioTemporalObjects.Point3D;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShapeSerde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

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

    private enum Type
    {
        SHAPE(0),
        CIRCLE(1),
        GEOMETRYCOLLECTION(2),
        ENVELOPE(3),
        POINT3D(4);

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
        else if (object instanceof Point3D) {
            writeType(out, Type.POINT3D);
            out.writeDouble(((Point3D) object).z);
            Geometry geometry = ((Point3D) object).point;
            writeGeometry(kryo, out, geometry);
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
            case POINT3D: {
                double z = input.readDouble();
                Geometry centerGeometry = readGeometry(kryo, input);
                Point3D point3D = new Point3D((Point) centerGeometry, z);
                return point3D;
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
}
