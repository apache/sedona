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
package org.apache.sedona.core.formatMapper.shapefileParser.parseUtils.shp;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.locationtech.jts.geom.GeometryFactory;

public enum ShapeType implements Serializable {
  // The following IDs are defined in Shapefile specification
  NULL(0, false),
  POINT(1, true),
  POLYLINE(3, true),
  POLYGON(5, true),
  MULTIPOINT(8, true),
  POINTZ(11, true),
  POLYLINEZ(13, true),
  POLYGONZ(15, true),
  MULTIPOINTZ(18, true),
  POINTM(21, true),
  POLYLINEM(23, true),
  POLYGONM(25, true),
  MULTIPOINTM(28, true),
  MULTIPATCH(31, false),
  // A normal shapefile should NOT have UNDEFINED type
  UNDEFINED(-1, false);

  private final int id;
  private final boolean supported;
  // A lookup map for getting a Type from its id
  private static final Map<Integer, ShapeType> lookup = new HashMap<Integer, ShapeType>();

  static {
    for (ShapeType s : ShapeType.values()) {
      lookup.put(s.id, s);
    }
  }

  ShapeType(int id, boolean supported) {
    this.id = id;
    this.supported = supported;
  }

  /**
   * return the corresponding ShapeType instance by int id.
   *
   * @param id the id
   * @return the type
   */
  public static ShapeType getType(int id) {
    ShapeType type = lookup.get(id);
    return type == null ? UNDEFINED : type;
  }

  /**
   * generate a parser according to current shape type.
   *
   * @param geometryFactory the geometry factory
   * @return the parser
   */
  public ShapeParser getParser(GeometryFactory geometryFactory) {
    switch (this) {
      case POINT:
      case POINTZ:
      case POINTM:
        return new PointParser(geometryFactory, this);
      case POLYLINE:
      case POLYLINEZ:
      case POLYLINEM:
        return new PolyLineParser(geometryFactory, this);
      case POLYGON:
      case POLYGONZ:
      case POLYGONM:
        return new PolygonParser(geometryFactory, this);
      case MULTIPOINT:
      case MULTIPOINTZ:
      case MULTIPOINTM:
        return new MultiPointParser(geometryFactory, this);
      default:
        throw new TypeUnknownException(id);
    }
  }

  /**
   * return the shape type id.
   *
   * @return the id
   */
  public int getId() {
    return id;
  }

  /**
   * return whether the shape type is supported by Sedona
   *
   * @return
   */
  public boolean isSupported() {
    return supported;
  }
}
