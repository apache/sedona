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

import java.util.*;


import com.vividsolutions.jts.algorithm.*;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.noding.*;
import com.vividsolutions.jts.geom.util.*;

/**
 * A base class containing the logic for computes the <tt>contains</tt>
 * and <tt>covers</tt> spatial relationship predicates
 * for a {@link PreparedPolygon} relative to all other {@link Geometry} classes.
 * Uses short-circuit tests and indexing to improve performance. 
 * <p>
 * Contains and covers are very similar, and differ only in how certain
 * cases along the boundary are handled.  These cases require 
 * full topological evaluation to handle, so all the code in 
 * this class is common to both predicates.
 * <p>
 * It is not possible to short-circuit in all cases, in particular
 * in the case where line segments of the test geometry touches the polygon linework.
 * In this case full topology must be computed.
 * (However, if the test geometry consists of only points, this 
 * <i>can</i> be evaluated in an optimized fashion.
 * 
 * @author Martin Davis
 *
 */
abstract class AbstractPreparedPolygonContains 
	extends PreparedPolygonPredicate
{
	/**
	 * This flag controls a difference between contains and covers.
	 * 
	 * For contains the value is true.
	 * For covers the value is false.
	 */
	protected boolean requireSomePointInInterior = true; 
	
	// information about geometric situation
	private boolean hasSegmentIntersection = false;
  private boolean hasProperIntersection = false;
  private boolean hasNonProperIntersection = false;
	
  /**
   * Creates an instance of this operation.
   * 
   * @param prepPoly the PreparedPolygon to evaluate
   */
	public AbstractPreparedPolygonContains(PreparedPolygon prepPoly)
	{
		super(prepPoly);
	}
	
	/**
	 * Evaluate the <tt>contains</tt> or <tt>covers</tt> relationship
	 * for the given geometry.
	 * 
	 * @param geom the test geometry
	 * @return true if the test geometry is contained
	 */
	protected boolean eval(Geometry geom)
	{
		/**
		 * Do point-in-poly tests first, since they are cheaper and may result
		 * in a quick negative result.
		 * 
		 * If a point of any test components does not lie in target, result is false
		 */
		boolean isAllInTargetArea = isAllTestComponentsInTarget(geom);
		if (! isAllInTargetArea) return false;
		
		/**
		 * If the test geometry consists of only Points, 
		 * then it is now sufficient to test if any of those
		 * points lie in the interior of the target geometry.
		 * If so, the test is contained.
		 * If not, all points are on the boundary of the area,
		 * which implies not contained.
		 */
		if (requireSomePointInInterior
				&& geom.getDimension() == 0) {
			boolean isAnyInTargetInterior = isAnyTestComponentInTargetInterior(geom);
			return isAnyInTargetInterior;
		}
		
		/**
		 * Check if there is any intersection between the line segments
		 * in target and test.
		 * In some important cases, finding a proper interesection implies that the 
		 * test geometry is NOT contained.
		 * These cases are:
		 * <ul>
		 * <li>If the test geometry is polygonal
		 * <li>If the target geometry is a single polygon with no holes
		 * <ul>
		 * In both of these cases, a proper intersection implies that there
		 * is some portion of the interior of the test geometry lying outside
		 * the target, which means that the test is not contained.
		 */
		boolean properIntersectionImpliesNotContained = isProperIntersectionImpliesNotContainedSituation(geom);
		// MD - testing only
//		properIntersectionImpliesNotContained = true;
		
    // find all intersection types which exist
    findAndClassifyIntersections(geom);
		
		if (properIntersectionImpliesNotContained && hasProperIntersection)
			return false;
		
    /**
     * If all intersections are proper 
     * (i.e. no non-proper intersections occur)
     * we can conclude that the test geometry is not contained in the target area,
     * by the Epsilon-Neighbourhood Exterior Intersection condition.
     * In real-world data this is likely to be by far the most common situation, 
     * since natural data is unlikely to have many exact vertex segment intersections.
     * Thus this check is very worthwhile, since it avoid having to perform
     * a full topological check.
     * 
     * (If non-proper (vertex) intersections ARE found, this may indicate
     * a situation where two shells touch at a single vertex, which admits
     * the case where a line could cross between the shells and still be wholely contained in them.
     */
		if (hasSegmentIntersection && ! hasNonProperIntersection)
      return false;
    
		/**
		 * If there is a segment intersection and the situation is not one
		 * of the ones above, the only choice is to compute the full topological
		 * relationship.  This is because contains/covers is very sensitive 
		 * to the situation along the boundary of the target.
		 */
		if (hasSegmentIntersection) {
			return fullTopologicalPredicate(geom);
//			System.out.println(geom);
		}
				
		/**
		 * This tests for the case where a ring of the target lies inside
		 * a test polygon - which implies the exterior of the Target
		 * intersects the interior of the Test, and hence the result is false
		 */
		if (geom instanceof Polygonal) {
			// TODO: generalize this to handle GeometryCollections
			boolean isTargetInTestArea = isAnyTargetComponentInAreaTest(geom, prepPoly.getRepresentativePoints());
			if (isTargetInTestArea) return false;
		}
		return true;
	}
	
	private boolean isProperIntersectionImpliesNotContainedSituation(Geometry testGeom)
	{
    /**
     * If the test geometry is polygonal we have the A/A situation.
     * In this case, a proper intersection indicates that 
     * the Epsilon-Neighbourhood Exterior Intersection condition exists.
     * This condition means that in some small
     * area around the intersection point, there must exist a situation
     * where the interior of the test intersects the exterior of the target.
     * This implies the test is NOT contained in the target. 
     */
		if (testGeom instanceof Polygonal) return true;
    /**
     * A single shell with no holes allows concluding that 
     * a proper intersection implies not contained 
     * (due to the Epsilon-Neighbourhood Exterior Intersection condition) 
     */
		if (isSingleShell(prepPoly.getGeometry())) return true;
		return false;
	}
	
  /**
   * Tests whether a geometry consists of a single polygon with no holes.
   *  
   * @return true if the geometry is a single polygon with no holes
   */
	private boolean isSingleShell(Geometry geom)
	{
    // handles single-element MultiPolygons, as well as Polygons
		if (geom.getNumGeometries() != 1) return false;
		
		Polygon poly = (Polygon) geom.getGeometryN(0);
		int numHoles = poly.getNumInteriorRing();
		if (numHoles == 0) return true;
		return false;
	}
	
	private void findAndClassifyIntersections(Geometry geom)
	{
    List lineSegStr = SegmentStringUtil.extractSegmentStrings(geom);
    
		SegmentIntersectionDetector intDetector = new SegmentIntersectionDetector();
		intDetector.setFindAllIntersectionTypes(true);
		prepPoly.getIntersectionFinder().intersects(lineSegStr, intDetector);
			
		hasSegmentIntersection = intDetector.hasIntersection();
    hasProperIntersection = intDetector.hasProperIntersection();
    hasNonProperIntersection = intDetector.hasNonProperIntersection();
	}
			
	/**
	 * Computes the full topological predicate.
	 * Used when short-circuit tests are not conclusive.
	 * 
	 * @param geom the test geometry
	 * @return true if this prepared polygon has the relationship with the test geometry
	 */
	protected abstract boolean fullTopologicalPredicate(Geometry geom);
	
}
