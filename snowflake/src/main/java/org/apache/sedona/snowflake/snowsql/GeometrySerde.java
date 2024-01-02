package org.apache.sedona.snowflake.snowsql;


import org.apache.sedona.common.Functions;
import org.apache.sedona.common.utils.GeomUtils;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

import java.util.Arrays;

public class GeometrySerde {

    public static GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    public static byte[] serialize(Geometry geom) {
        return Functions.asEWKB(geom);
    }

    public static byte[] serialize(Geometry[] geoms) {
        return serialize(Functions.createMultiGeometry(geoms));
    }

    public static Geometry deserialize(byte[] bytes) {
        try {
            return new WKBReader().read(bytes);
        } catch (ParseException e) {
            String msg= String.format("Failed to parse WKB(printed through Arrays.toString(bytes)): %s, error: %s", Arrays.toString(bytes), e.getMessage());
            throw new IllegalArgumentException(msg);
        }
    }

    public static Geometry[] deserialize2List(byte[] bytes) {
        Geometry geom = GeometrySerde.deserialize(bytes);
        assert geom instanceof GeometryCollection;
        return GeomUtils.getSubGeometries(geom);
    }
}
