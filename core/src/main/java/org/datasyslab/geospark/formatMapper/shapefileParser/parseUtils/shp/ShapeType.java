/*
 * FILE: ShapeType
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
package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp;

import com.vividsolutions.jts.geom.GeometryFactory;

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
