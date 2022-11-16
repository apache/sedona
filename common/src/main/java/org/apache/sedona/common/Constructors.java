package org.apache.sedona.common;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

public class Constructors {

    public static Geometry geomFromWKT(String wkt, int srid) throws ParseException {
        if (wkt == null) {
            return null;
        }
        GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), srid);
        return new WKTReader(geometryFactory).read(wkt);
    }
}
