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

/**
 * An interface for strategies for determining the location of split points on constraint segments.
 * The location of split points has a large effect on the performance and robustness of enforcing a
 * constrained Delaunay triangulation. Poorly chosen split points can cause repeated splitting,
 * especially at narrow constraint angles, since the split point will end up encroaching on the
 * segment containing the original encroaching point. With detailed knowledge of the geometry of the
 * constraints, it is sometimes possible to choose better locations for splitting.
 * 
 * @author mbdavis
 */
public interface ConstraintSplitPointFinder {
    /**
     * Finds a point at which to split an encroached segment to allow the original segment to appear
     * as edges in a constrained Delaunay triangulation.
     * 
     * @param seg the encroached segment
     * @param encroachPt the encroaching point
     * @return the point at which to split the encroached segment
     */
    Coordinate findSplitPoint(Segment seg, Coordinate encroachPt);
}
