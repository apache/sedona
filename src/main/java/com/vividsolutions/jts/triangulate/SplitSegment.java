/*
 * The JTS Topology Suite is a collection of Java classes that
 * implement the fundamental operations required to validate a given
 * geo-spatial data set to a known topological specification.
 *
 * Copyright (C) 2001 Vivid Solutions
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * For more information, contact:
 *
 *     Vivid Solutions
 *     Suite #1A
 *     2328 Government Street
 *     Victoria BC  V8T 5G5
 *     Canada
 *
 *     (250)385-6040
 *     www.vividsolutions.com
 */

package com.vividsolutions.jts.triangulate;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LineSegment;

/**
 * Models a constraint segment which can be split in two in various ways, 
 * according to certain geometric constraints.
 * 
 * @author Martin Davis
 */
public class SplitSegment {
    /**
     * Computes the {@link Coordinate} that lies a given fraction along the line defined by the
     * reverse of the given segment. A fraction of <code>0.0</code> returns the end point of the
     * segment; a fraction of <code>1.0</code> returns the start point of the segment.
     * 
     * @param seg the LineSegment
     * @param segmentLengthFraction the fraction of the segment length along the line
     * @return the point at that distance
     */
    private static Coordinate pointAlongReverse(LineSegment seg, double segmentLengthFraction) {
        Coordinate coord = new Coordinate();
        coord.x = seg.p1.x - segmentLengthFraction * (seg.p1.x - seg.p0.x);
        coord.y = seg.p1.y - segmentLengthFraction * (seg.p1.y - seg.p0.y);
        return coord;
    }

    private LineSegment seg;
    private double      segLen;
    private Coordinate  splitPt;
    private double      minimumLen = 0.0;

    public SplitSegment(LineSegment seg) {
        this.seg = seg;
        segLen = seg.getLength();
    }

    public void setMinimumLength(double minLen) {
        minimumLen = minLen;
    }

    public Coordinate getSplitPoint() {
        return splitPt;
    }

    public void splitAt(double length, Coordinate endPt) {
        double actualLen = getConstrainedLength(length);
        double frac = actualLen / segLen;
        if (endPt.equals2D(seg.p0))
            splitPt = seg.pointAlong(frac);
        else
            splitPt = pointAlongReverse(seg, frac);
    }

    public void splitAt(Coordinate pt) {
        // check that given pt doesn't violate min length
        double minFrac = minimumLen / segLen;
        if (pt.distance(seg.p0) < minimumLen) {
            splitPt = seg.pointAlong(minFrac);
            return;
        }
        if (pt.distance(seg.p1) < minimumLen) {
            splitPt = pointAlongReverse(seg, minFrac);
            return;
        }
        // passes minimum distance check - use provided point as split pt
        splitPt = pt;
    }

    private double getConstrainedLength(double len) {
        if (len < minimumLen)
            return minimumLen;
        return len;
    }

}
