package org.datasyslab.geospark.utils;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;

public class HalfOpenRectangle {
    private final Envelope envelope;

    public HalfOpenRectangle(Envelope envelope) {
        this.envelope = envelope;
    }

    public boolean contains(Point point) {
        return contains(point.getX(), point.getY());
    }

    public boolean contains(double x, double y) {
        return x >= envelope.getMinX() && x < envelope.getMaxX()
            && y >= envelope.getMinY() && y < envelope.getMaxY();
    }

    public Envelope getEnvelope() {
        return envelope;
    }
}
