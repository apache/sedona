package org.apache.sedona.snowflake.snowsql;

import org.apache.sedona.common.FunctionsGeoTools;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

public class GeoToolsWrapper {
    public static Geometry transform(Geometry geometry, String sourceCRS, String targetCRS, boolean lenient) {
        try {
            return FunctionsGeoTools.transform(
                    geometry,
                    sourceCRS,
                    targetCRS,
                    lenient
            );
        } catch (FactoryException | TransformException e) {
            throw new RuntimeException(e);
        }
    }

    public static Geometry transform(Geometry geometry, String sourceCRS, String targetCRS) {
        try {
            return FunctionsGeoTools.transform(
                    geometry,
                    sourceCRS,
                    targetCRS
            );
        } catch (FactoryException | TransformException e) {
            throw new RuntimeException(e);
        }
    }
}
