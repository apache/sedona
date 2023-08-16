package org.apache.sedona.common.utils;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

public class GeomUtilsGeoTools {
    public static Geometry transform(Geometry geometry, CoordinateReferenceSystem sourceCRS, CoordinateReferenceSystem targetCRS, boolean lenient) throws FactoryException, TransformException {
        MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS, lenient);
        return JTS.transform(geometry, transform);
    }
}
