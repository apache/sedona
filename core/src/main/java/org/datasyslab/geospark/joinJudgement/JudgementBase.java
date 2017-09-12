package org.datasyslab.geospark.joinJudgement;

import com.vividsolutions.jts.geom.Geometry;

import java.io.Serializable;

abstract class JudgementBase implements Serializable {
    private final boolean considerBoundaryIntersection;

    protected JudgementBase(boolean considerBoundaryIntersection) {
        this.considerBoundaryIntersection = considerBoundaryIntersection;
    }

    protected boolean match(Geometry polygon, Geometry geometry) {
        return considerBoundaryIntersection ? polygon.intersects(geometry) : polygon.covers(geometry);
    }
}
