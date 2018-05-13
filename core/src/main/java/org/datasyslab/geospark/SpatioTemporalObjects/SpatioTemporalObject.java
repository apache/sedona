package org.datasyslab.geospark.SpatioTemporalObjects;

import java.io.Serializable;

import org.apache.log4j.Logger;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

/**
 * The Class SpatioTemporalObject.
 */
public class SpatioTemporalObject
        implements Serializable
{

    /**
     * The Constant logger.
     */
    final static Logger logger = Logger.getLogger(SpatioTemporalObject.class);

    public Geometry geometry;

    public double beginTime;

    public double endTime;

    public SpatioTemporalObject(Geometry geometry, double beginTime, double endTime) {
        this.geometry = geometry;
        this.beginTime = beginTime;
        this.endTime = endTime;
    }

    public Cube getCubeInternal() {
        Cube envelope = new Cube(geometry.getEnvelopeInternal(), beginTime, endTime);
        return envelope;
    }

    public Envelope getEnvelopeInternal() {
        // SpatioTemporalEnvelope envelope = new SpatioTemporalEnvelope(this, geometry.getEnvelopeInternal());
        // return envelope;
        return geometry.getEnvelopeInternal();
    }

}
