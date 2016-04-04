package org.datasyslab.geospark.geometryObjects;

import com.vividsolutions.jts.geom.Envelope;

/**
 * Created by jinxuanwu on 1/7/16.
 */
public class EnvelopeWithGrid extends Envelope{
    public int grid;

    public EnvelopeWithGrid(double x1, double x2, double y1, double y2, int grid) {
        super(x1, x2, y1, y2);
        this.grid = grid;
    }

    public EnvelopeWithGrid(Envelope e, int grid) {
        super(e);
        this.grid = grid;
    }
}
