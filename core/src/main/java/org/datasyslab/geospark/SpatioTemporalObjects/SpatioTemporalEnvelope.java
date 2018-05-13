package org.datasyslab.geospark.SpatioTemporalObjects;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;

/**
 * The SpatioTemporalEnvelope class
 */
public class SpatioTemporalEnvelope {

    public SpatioTemporalObject spatioTemporalObject;

    public Envelope envelope;

    public SpatioTemporalEnvelope(SpatioTemporalObject spatioTemporalObject, Envelope envelope) {
        this.spatioTemporalObject = spatioTemporalObject;
        this.envelope = envelope;
    }

    public int hashCode()
    {
        return envelope.hashCode();
    }

    public double getWidth() {
        return envelope.getWidth();
    }

    public double getHeight() {
        return envelope.getHeight();
    }

    public double getMinX() {
        return envelope.getMinX();
    }

    public double getMaxX() {
        return envelope.getMaxX();
    }

    public double getMinY() {
        return envelope.getMinY();
    }

    public double getMaxY() {
        return envelope.getMaxY();
    }

    public double getArea() {
        return envelope.getArea();
    }

    public double minExtent() {
        return envelope.minExtent();
    }

    public double maxExtent() {
        return envelope.maxExtent();
    }


    public Coordinate centre() {
        return envelope.centre();
    }

    public Envelope intersection(Envelope env) {
        return envelope.intersection(env);
    }

    public boolean intersects(Envelope other) {
        return envelope.intersects(other);
    }

    /**
     * @deprecated
     */
    public boolean overlaps(Envelope other) {
        return envelope.overlaps(other);
    }

    public boolean intersects(Coordinate p) {
        return envelope.intersects(p);
    }

    /**
     * @deprecated
     */
    public boolean overlaps(Coordinate p) {
        return envelope.overlaps(p);
    }

    public boolean intersects(double x, double y) {
        return envelope.intersects(x, y);
    }

    /**
     * @deprecated
     */
    public boolean overlaps(double x, double y) {
        return envelope.overlaps(x, y);
    }

    public boolean contains(Envelope other) {
        return envelope.contains(other);
    }

    public boolean contains(Coordinate p) {
        return envelope.contains(p);
    }

    public boolean contains(double x, double y) {
        return envelope.contains(x, y);
    }

    public boolean covers(double x, double y) {
        return envelope.covers(x, y);
    }

    public boolean covers(Coordinate p) {
        return envelope.covers(p);
    }

    public boolean covers(Envelope other) {
        return envelope.covers(other);
    }

    public double distance(Envelope env) {
        return envelope.distance(env);
    }

    public boolean equals(java.lang.Object other) {
        return envelope.equals(other);
    }

    public java.lang.String toString() {
        return envelope.toString();
    }

    public int compareTo(java.lang.Object o) {
        return envelope.compareTo(o);
    }

    public java.lang.Object getUserData() {
        return envelope.getUserData();
    }

    public com.vividsolutions.jts.geom.Geometry getOriginalGeometry() {
        return envelope.getOriginalGeometry();
    }

}
