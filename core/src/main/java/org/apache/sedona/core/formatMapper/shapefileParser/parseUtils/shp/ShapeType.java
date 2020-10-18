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

import org.locationtech.jts.geom.GeometryFactory;

import java.io.Serializable;

public enum ShapeType
        implements Serializable
{

    UNDEFINED(0),
    POINT(1),
    POLYLINE(3),
    POLYGON(5),
    MULTIPOINT(8);

    private final int id;

    ShapeType(int id)
    {
        this.id = id;
    }

    /**
     * return the corresponding ShapeType instance by int id.
     *
     * @param id the id
     * @return the type
     */
    public static ShapeType getType(int id)
    {
        ShapeType type;
        switch (id) {
            case 1:
                type = POINT;
                break;
            case 3:
                type = POLYLINE;
                break;
            case 5:
                type = POLYGON;
                break;
            case 8:
                type = MULTIPOINT;
                break;
            default:
                type = UNDEFINED;
        }
        return type;
    }

    /**
     * generate a parser according to current shape type.
     *
     * @param geometryFactory the geometry factory
     * @return the parser
     */
    public ShapeParser getParser(GeometryFactory geometryFactory)
    {
        switch (this) {
            case POINT:
                return new PointParser(geometryFactory);
            case POLYLINE:
                return new PolyLineParser(geometryFactory);
            case POLYGON:
                return new PolygonParser(geometryFactory);
            case MULTIPOINT:
                return new MultiPointParser(geometryFactory);
            default:
                throw new TypeUnknownException(id);
        }
    }

    /**
     * return the shape type id.
     *
     * @return the id
     */
    public int getId()
    {
        return id;
    }
}
