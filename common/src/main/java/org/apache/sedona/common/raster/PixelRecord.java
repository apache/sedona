package org.apache.sedona.common.raster;

import org.locationtech.jts.geom.Geometry;

public class PixelRecord {
    public final Geometry geom;
    public final double value;
    public final int colX, rowY;

    public PixelRecord(Geometry geom, double value, int colX, int rowY) {
        this.geom = geom;
        this.value = value;
        this.colX = colX;
        this.rowY = rowY;
    }
}
