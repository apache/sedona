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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.Serializable;
import org.apache.sedona.common.geometryObjects.Circle;
import org.apache.sedona.common.geometryObjects.Geography;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

/**
 * Provides methods to efficiently serialize and deserialize geometry types.
 *
 * <p>Supports Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon,
 * GeometryCollection, Circle, Envelope, and Geography types.
 *
 * <p>First byte contains {@link Type#id}. Then go type-specific bytes, followed by user-data
 * attached to the geometry.
 */
public class GeometrySerde extends Serializer implements Serializable {
  @Override
  public void write(Kryo kryo, Output out, Object object) {
    if (object instanceof Circle) {
      Circle circle = (Circle) object;
      writeType(out, Type.CIRCLE);
      out.writeDouble(circle.getRadius());
      writeGeometry(kryo, out, circle.getCenterGeometry());
      writeUserData(kryo, out, circle);
    } else if (object instanceof Point
        || object instanceof LineString
        || object instanceof Polygon
        || object instanceof GeometryCollection) {
      writeType(out, Type.SHAPE);
      writeGeometry(kryo, out, (Geometry) object);
    } else if (object instanceof Envelope) {
      Envelope envelope = (Envelope) object;
      writeType(out, Type.ENVELOPE);
      out.writeDouble(envelope.getMinX());
      out.writeDouble(envelope.getMaxX());
      out.writeDouble(envelope.getMinY());
      out.writeDouble(envelope.getMaxY());
    } else if (object instanceof Geography) {
      writeType(out, Type.GEOGRAPHY);
      writeGeometry(kryo, out, ((Geography) object).getGeometry());
    } else {
      throw new UnsupportedOperationException(
          "Cannot serialize object of type " + object.getClass().getName());
    }
  }

  private void writeType(Output out, Type type) {
    out.writeByte((byte) type.id);
  }

  private void writeGeometry(Kryo kryo, Output out, Geometry geometry) {
    byte[] data = GeometrySerializer.serialize(geometry);
    out.writeInt(data.length);
    out.write(data, 0, data.length);
    writeUserData(kryo, out, geometry);
  }

  private void writeUserData(Kryo kryo, Output out, Geometry geometry) {
    out.writeBoolean(geometry.getUserData() != null);
    if (geometry.getUserData() != null) {
      kryo.writeClass(out, geometry.getUserData().getClass());
      kryo.writeObject(out, geometry.getUserData());
    }
  }

  @Override
  public Object read(Kryo kryo, Input input, Class aClass) {
    byte typeId = input.readByte();
    Type geometryType = Type.fromId(typeId);
    switch (geometryType) {
      case SHAPE:
        return readGeometry(kryo, input);
      case CIRCLE:
        {
          double radius = input.readDouble();
          Geometry centerGeometry = readGeometry(kryo, input);
          Object userData = readUserData(kryo, input);

          Circle circle = new Circle(centerGeometry, radius);
          circle.setUserData(userData);
          return circle;
        }
      case ENVELOPE:
        {
          double xMin = input.readDouble();
          double xMax = input.readDouble();
          double yMin = input.readDouble();
          double yMax = input.readDouble();
          if (xMin <= xMax) {
            return new Envelope(xMin, xMax, yMin, yMax);
          } else {
            // Null envelope cannot be constructed using Envelope(xMin, xMax, yMin, yMax)
            return new Envelope();
          }
        }
      case GEOGRAPHY:
        {
          return new Geography(readGeometry(kryo, input));
        }
      default:
        throw new UnsupportedOperationException(
            "Cannot deserialize object of type " + geometryType);
    }
  }

  private Object readUserData(Kryo kryo, Input input) {
    Object userData = null;
    if (input.readBoolean()) {
      Registration clazz = kryo.readClass(input);
      userData = kryo.readObject(input, clazz.getType());
    }
    return userData;
  }

  private Geometry readGeometry(Kryo kryo, Input input) {
    int length = input.readInt();
    byte[] bytes = new byte[length];
    input.readBytes(bytes);
    Geometry geometry = GeometrySerializer.deserialize(bytes);
    geometry.setUserData(readUserData(kryo, input));
    return geometry;
  }

  private enum Type {
    SHAPE(0),
    CIRCLE(1),
    ENVELOPE(2),
    GEOGRAPHY(3);

    private final int id;

    Type(int id) {
      this.id = id;
    }

    public static Type fromId(int id) {
      for (Type type : values()) {
        if (type.id == id) {
          return type;
        }
      }

      return null;
    }
  }
}
