
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
package com.vividsolutions.jts.noding.snapround;

import java.util.*;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.algorithm.*;
import com.vividsolutions.jts.noding.*;

/**
 * Uses Snap Rounding to compute a rounded,
 * fully noded arrangement from a set of {@link SegmentString}s.
 * Implements the Snap Rounding technique described in 
 * papers by Hobby, Guibas & Marimont, and Goodrich et al.
 * Snap Rounding assumes that all vertices lie on a uniform grid;
 * hence the precision model of the input must be fixed precision,
 * and all the input vertices must be rounded to that precision.
 * <p>
 * This implementation uses a monotone chains and a spatial index to
 * speed up the intersection tests.
 * <p>
 * This implementation appears to be fully robust using an integer precision model.
 * It will function with non-integer precision models, but the
 * results are not 100% guaranteed to be correctly noded.
 *
 * @version 1.7
 */
public class MCIndexSnapRounder
    implements Noder
{
  private final PrecisionModel pm;
  private LineIntersector li;
  private final double scaleFactor;
  private MCIndexNoder noder;
  private MCIndexPointSnapper pointSnapper;
  private Collection nodedSegStrings;

  public MCIndexSnapRounder(PrecisionModel pm) {
    this.pm = pm;
    li = new RobustLineIntersector();
    li.setPrecisionModel(pm);
    scaleFactor = pm.getScale();
  }

  public Collection getNodedSubstrings()
  {
    return  NodedSegmentString.getNodedSubstrings(nodedSegStrings);
  }

  public void computeNodes(Collection inputSegmentStrings)
  {
    this.nodedSegStrings = inputSegmentStrings;
    noder = new MCIndexNoder();
    pointSnapper = new MCIndexPointSnapper(noder.getIndex());
    snapRound(inputSegmentStrings, li);

    // testing purposes only - remove in final version
    //checkCorrectness(inputSegmentStrings);
  }

  private void checkCorrectness(Collection inputSegmentStrings)
  {
    Collection resultSegStrings = NodedSegmentString.getNodedSubstrings(inputSegmentStrings);
    NodingValidator nv = new NodingValidator(resultSegStrings);
    try {
      nv.checkValid();
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  private void snapRound(Collection segStrings, LineIntersector li)
  {
    List intersections = findInteriorIntersections(segStrings, li);
    computeIntersectionSnaps(intersections);
    computeVertexSnaps(segStrings);
  }

  /**
   * Computes all interior intersections in the collection of {@link SegmentString}s,
   * and returns their @link Coordinate}s.
   *
   * Does NOT node the segStrings.
   *
   * @return a list of Coordinates for the intersections
   */
  private List findInteriorIntersections(Collection segStrings, LineIntersector li)
  {
    InteriorIntersectionFinderAdder intFinderAdder = new InteriorIntersectionFinderAdder(li);
    noder.setSegmentIntersector(intFinderAdder);
    noder.computeNodes(segStrings);
    return intFinderAdder.getInteriorIntersections();
  }

  /**
   * Snaps segments to nodes created by segment intersections.
   */
  private void computeIntersectionSnaps(Collection snapPts)
  {
    for (Iterator it = snapPts.iterator(); it.hasNext(); ) {
      Coordinate snapPt = (Coordinate) it.next();
      HotPixel hotPixel = new HotPixel(snapPt, scaleFactor, li);
      pointSnapper.snap(hotPixel);
    }
  }

  /**
   * Snaps segments to all vertices.
   *
   * @param edges the list of segment strings to snap together
   */
  public void computeVertexSnaps(Collection edges)
  {
    for (Iterator i0 = edges.iterator(); i0.hasNext(); ) {
      NodedSegmentString edge0 = (NodedSegmentString) i0.next();
      computeVertexSnaps(edge0);
    }
  }

  /**
   * Snaps segments to the vertices of a Segment String.  
   */
  private void computeVertexSnaps(NodedSegmentString e)
  {
    Coordinate[] pts0 = e.getCoordinates();
    for (int i = 0; i < pts0.length ; i++) {
      HotPixel hotPixel = new HotPixel(pts0[i], scaleFactor, li);
      boolean isNodeAdded = pointSnapper.snap(hotPixel, e, i);
      // if a node is created for a vertex, that vertex must be noded too
      if (isNodeAdded) {
        e.addIntersection(pts0[i], i);
      }
  }
}

}
