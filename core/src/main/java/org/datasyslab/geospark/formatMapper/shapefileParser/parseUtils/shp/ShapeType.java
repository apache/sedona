/**
 * FILE: ShapeType.java
 * PATH: org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShapeType.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
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
