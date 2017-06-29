/**
 * FILE: PairGeometry.java
 * PATH: org.datasyslab.geospark.geometryObjects.PairGeometry.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.geometryObjects;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashSet;

public class PairGeometry implements Serializable {
    public Geometry keyGeometry;
    public HashSet valueGeometries;
    public PairGeometry(Geometry keyGeometry, HashSet valueGeometries)
    {
        this.keyGeometry = keyGeometry;
        this.valueGeometries = valueGeometries;
    }
    public Tuple2<Polygon,HashSet<Geometry>> getPolygonTuple2()
    {
        return new Tuple2<Polygon,HashSet<Geometry>>((Polygon) keyGeometry,valueGeometries);
    }
    public Tuple2<Point,HashSet<Geometry>> getPointTuple2()
    {
        return new Tuple2<Point,HashSet<Geometry>>((Point) keyGeometry,valueGeometries);
    }
    public Tuple2<LineString,HashSet<Geometry>> getLineStringTuple2()
    {
        return new Tuple2<LineString,HashSet<Geometry>>((LineString) keyGeometry,valueGeometries);
    }
    public Tuple2<Circle,HashSet<Geometry>> getCircleTuple2()
    {
        return new Tuple2<Circle,HashSet<Geometry>>((Circle) keyGeometry,valueGeometries);
    }

    @Override
    public boolean equals(Object o)
    {
        PairGeometry anotherPairGeometry = (PairGeometry)o;
        if (keyGeometry.equals(((PairGeometry) o).keyGeometry)&&valueGeometries.equals(((PairGeometry) o).valueGeometries))
        {
            return true;
        }
        return false;

    }

    @Override
    public int hashCode()
    {
        return keyGeometry.hashCode()*31+valueGeometries.hashCode();
    }
}
