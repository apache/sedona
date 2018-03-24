package org.datasyslab.geospark.rangeJudgement;

import com.vividsolutions.jts.geom.Geometry;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.Serializable;

public class JudgementBase<U extends Geometry>
        implements Serializable
{

    private static final Logger log = LogManager.getLogger(JudgementBase.class);
    private boolean considerBoundaryIntersection;
    U queryGeometry;
    protected boolean leftCoveredByRight = true;

    /**
     * Instantiates a new range filter using index.
     *
     * @param queryWindow the query window
     * @param considerBoundaryIntersection the consider boundary intersection
     */
    public JudgementBase(U queryWindow, boolean considerBoundaryIntersection, boolean leftCoveredByRight)
    {
        this.considerBoundaryIntersection = considerBoundaryIntersection;
        this.queryGeometry = queryWindow;
        this.leftCoveredByRight = leftCoveredByRight;
    }

    public boolean match(Geometry spatialObject, Geometry queryWindow)
    {
        if (considerBoundaryIntersection) {
            if (queryWindow.intersects(spatialObject)) { return true; }
        }
        else {
            if (queryWindow.covers(spatialObject)) { return true; }
        }
        return false;
    }
}
