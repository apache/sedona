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
package com.vividsolutions.jts.geom.prep;

import java.util.List;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.noding.SegmentStringUtil;

/**
 * Computes the <tt>intersects</tt> spatial relationship predicate for
 * {@link PreparedPolygon}s relative to all other {@link Geometry} classes. Uses
 * short-circuit tests and indexing to improve performance.
 * 
 * @author Martin Davis
 * 
 */
class PreparedPolygonIntersects extends PreparedPolygonPredicate {
  /**
   * Computes the intersects predicate between a {@link PreparedPolygon} and a
   * {@link Geometry}.
   * 
   * @param prep
   *          the prepared polygon
   * @param geom
   *          a test geometry
   * @return true if the polygon intersects the geometry
   */
  public static boolean intersects(PreparedPolygon prep, Geometry geom) {
    PreparedPolygonIntersects polyInt = new PreparedPolygonIntersects(prep);
    return polyInt.intersects(geom);
  }

  /**
   * Creates an instance of this operation.
   * 
   * @param prepPoly
   *          the PreparedPolygon to evaluate
   */
  public PreparedPolygonIntersects(PreparedPolygon prepPoly) {
    super(prepPoly);
  }

  /**
   * Tests whether this PreparedPolygon intersects a given geometry.
   * 
   * @param geom
   *          the test geometry
   * @return true if the test geometry intersects
   */
  public boolean intersects(Geometry geom) {
    /**
     * Do point-in-poly tests first, since they are cheaper and may result in a
     * quick positive result.
     * 
     * If a point of any test components lie in target, result is true
     */
    boolean isInPrepGeomArea = isAnyTestComponentInTarget(geom);
    if (isInPrepGeomArea)
      return true;
    /**
     * If input contains only points, then at
     * this point it is known that none of them are contained in the target
     */
    if (geom.getDimension() == 0)
      return false;
    /**
     * If any segments intersect, result is true
     */
    List lineSegStr = SegmentStringUtil.extractSegmentStrings(geom);
    // only request intersection finder if there are segments 
    // (i.e. NOT for point inputs)
    if (lineSegStr.size() > 0) {
      boolean segsIntersect = prepPoly.getIntersectionFinder().intersects(
          lineSegStr);
      if (segsIntersect)
        return true;
    }

    /**
     * If the test has dimension = 2 as well, it is necessary to test for proper
     * inclusion of the target. Since no segments intersect, it is sufficient to
     * test representative points.
     */
    if (geom.getDimension() == 2) {
      // TODO: generalize this to handle GeometryCollections
      boolean isPrepGeomInArea = isAnyTargetComponentInAreaTest(geom,
          prepPoly.getRepresentativePoints());
      if (isPrepGeomInArea)
        return true;
    }

    return false;
  }

}
