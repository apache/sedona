package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.ShapeFileConst;
import org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.ShapeReader;

import java.io.IOException;
import java.io.Serializable;

/**
 * Created by zongsizhang on 6/19/17.
 */
public abstract class ShapeParser implements Serializable, ShapeFileConst{

    protected GeometryFactory geometryFactory = null;

    public ShapeParser(GeometryFactory geometryFactory) {
        this.geometryFactory = geometryFactory;
    }

    /**
     * parse the shape to a geometry
     * @param reader
     */
    public abstract Geometry parserShape(ShapeReader reader) throws IOException;

}
