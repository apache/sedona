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

package com.vividsolutions.jts.triangulate.quadedge;

/**
 * Interface for classes which process triangles visited during travesals of a
 * {@link QuadEdgeSubdivision}
 * 
 * @author Martin Davis
 */
public interface TraversalVisitor {
    /**
     * Visits a triangle during a traversal of a {@link QuadEdgeSubdivision}. An implementation of
     * this method may perform processing on the current triangle. It must also decide whether a
     * neighbouring triangle should be added to the queue so its neighbours are visited. Often it
     * will perform processing on the neighbour triangle as well, in order to mark it as processed
     * (visited) and/or to determine if it should be visited. Note that choosing <b>not</b> to
     * visit the neighbouring triangle is the terminating condition for many traversal algorithms.
     * In particular, if the neighbour triangle has already been visited, it should not be visited
     * again.
     * 
     * @param currTri the current triangle being processed
     * @param edgeIndex the index of the edge in the current triangle being traversed
     * @param neighbTri a neighbouring triangle next in line to visit
     * @return true if the neighbour triangle should be visited
     */
    boolean visit(QuadEdgeTriangle currTri, int edgeIndex, QuadEdgeTriangle neighbTri);
}
