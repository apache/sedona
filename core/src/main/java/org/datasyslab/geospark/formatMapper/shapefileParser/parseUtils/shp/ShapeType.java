/**
 * FILE: ShapeType.java
 * PATH: org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShapeType.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import java.io.Serializable;

// TODO: Auto-generated Javadoc
/**
 * The Class ShapeType.
 */
public class ShapeType implements Serializable{

    /** The id. */
    protected final int id;

    /** The Constant UNDEFINED. */
    public static final ShapeType UNDEFINED = new ShapeType(0);

    /** The Constant NULL. */
    public static final ShapeType NULL = new ShapeType(0);

    /** The Constant POINT. */
    public static final ShapeType POINT = new ShapeType(1);

    /** The Constant POLYLINE. */
    public static final ShapeType POLYLINE = new ShapeType(3);

    /** The Constant POLYGON. */
    public static final ShapeType POLYGON = new ShapeType(5);

    /** The Constant MULTIPOINT. */
    public static final ShapeType MULTIPOINT = new ShapeType(8);

    /**
     * Instantiates a new shape type.
     *
     * @param i the i
     */
    protected ShapeType(int i){
        id = i;
    }

    /**
     * return the corresponding ShapeType instance by int id.
     *
     * @param idx the idx
     * @return the type
     */
    public static ShapeType getType(int idx){
        ShapeType type;
        switch(idx){
            case 0:
                type = NULL;
                break;
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
    public ShapeParser getParser(GeometryFactory geometryFactory){
        ShapeParser parser = null;
        switch (id){
            case 1:
                parser = new PointParser(geometryFactory);
                break;
            case 3:
                parser = new PolyLineParser(geometryFactory);
                break;
            case 5:
                parser = new PolygonParser(geometryFactory);
                break;
            case 8:
                parser = new MultiPointParser(geometryFactory);
                break;
            default:
                parser = null;
        }
        return parser;
    }

    /**
     * return the shape type id.
     *
     * @return the id
     */
    public int getId() {
        return id;
    }
}
