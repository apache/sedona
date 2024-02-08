package org.apache.sedona.common.utils;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

public class CRSUtils {
    // Standard EPSG Codes
    public static final int EPSG_WORLD_MERCATOR = 3395; // Replacing SRID_WORLD_MERCATOR
    public static final int EPSG_NORTH_UTM_START = 32601; // Replacing SRID_NORTH_UTM_START
    public static final int EPSG_NORTH_UTM_END = 32660; // Replacing SRID_NORTH_UTM_END
    public static final int EPSG_NORTH_LAMBERT = 3574; // Replacing SRID_NORTH_LAMBERT with EPSG:3574 (North Pole LAEA Atlantic)
    public static final int EPSG_NORTH_STEREO = 3995; // Replacing SRID_NORTH_STEREO with ESRI:102017 (WGS 1984 North Pole LAEA)
    public static final int EPSG_SOUTH_UTM_START = 32701; // Replacing SRID_SOUTH_UTM_START
    public static final int EPSG_SOUTH_UTM_END = 32760; // Replacing SRID_SOUTH_UTM_END
    public static final int EPSG_SOUTH_LAMBERT = 3409; // Replacing SRID_SOUTH_LAMBERT with EPSG:3409 (South Pole LAEA)
    public static final int EPSG_SOUTH_STEREO = 3031; // Replacing SRID_SOUTH_STEREO

    private CRSUtils() {}

    public static int bestSRID(Geometry geometry) {
        Envelope envelope = geometry.getEnvelopeInternal();
        if (envelope.isNull()) return EPSG_WORLD_MERCATOR; // Fallback EPSG

        // Center of the bounding box
        double centerX = (envelope.getMinX() + envelope.getMaxX()) / 2.0;
        double centerY = (envelope.getMinY() + envelope.getMaxY()) / 2.0;

        // Width and height in degrees
        double xwidth = envelope.getWidth();
        double ywidth = envelope.getHeight();

        // Check for polar regions
        if (centerY > 70.0 && ywidth < 45.0) return EPSG_NORTH_LAMBERT;
        if (centerY < -70.0 && ywidth < 45.0) return EPSG_SOUTH_LAMBERT;

        // Check for UTM zones
        if (xwidth < 6.0) {
            int zone = (int)Math.floor((centerX + 180.0) / 6.0);
            zone = Math.min(zone, 59);
            return (centerY < 0.0) ? EPSG_SOUTH_UTM_START + zone : EPSG_NORTH_UTM_START + zone;
        }

        // Default fallback
        return EPSG_WORLD_MERCATOR;
    }
}
