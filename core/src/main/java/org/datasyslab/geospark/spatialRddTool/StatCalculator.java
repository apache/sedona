package org.datasyslab.geospark.spatialRddTool;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import java.io.Serializable;
import java.util.Objects;

public class StatCalculator implements Serializable {
    private Envelope boundary;
    private long count;

    public StatCalculator(Envelope boundary, long count)
    {
        Objects.requireNonNull(boundary, "Boundary cannot be null");
        if (count <= 0) {
            throw new IllegalArgumentException("Count must be > 0");
        }
        this.boundary = boundary;
        this.count = count;
    }
    public static StatCalculator combine(StatCalculator agg1, StatCalculator agg2) throws Exception {
        if (agg1 == null) {
            return agg2;
        }

        if (agg2 == null) {
            return agg1;
        }

        return new StatCalculator(
                StatCalculator.combine(agg1.boundary, agg2.boundary),
                agg1.count + agg2.count);
    }
    public static Envelope combine(Envelope agg1, Envelope agg2) throws Exception {
        if (agg1 == null) {
            return agg2;
        }

        if (agg2 == null) {
            return agg1;
        }

        return new Envelope(
                Math.min(agg1.getMinX(), agg2.getMinX()),
                Math.max(agg1.getMaxX(), agg2.getMaxX()),
                Math.min(agg1.getMinY(), agg2.getMinY()),
                Math.max(agg1.getMaxY(), agg2.getMaxY()));
    }

    public static Envelope add(Envelope agg, Geometry object) throws Exception {
        return combine(object.getEnvelopeInternal(), agg);
    }

    public static StatCalculator add(StatCalculator agg, Geometry object) throws Exception {
        return combine(new StatCalculator(object.getEnvelopeInternal(), 1), agg);
    }
    public Envelope getBoundary() {
        return boundary;
    }

    public long getCount() {
        return count;
    }
}
