package org.datasyslab.geospark.joinJudgement;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;

import java.io.Serializable;

abstract class JudgementBase implements Serializable {
    private final boolean considerBoundaryIntersection;

    protected JudgementBase(boolean considerBoundaryIntersection) {
        this.considerBoundaryIntersection = considerBoundaryIntersection;
    }

    protected boolean match(Polygon polygon, Geometry geometry) {
        return considerBoundaryIntersection ? polygon.intersects(geometry) : polygon.covers(geometry);
    }
}
