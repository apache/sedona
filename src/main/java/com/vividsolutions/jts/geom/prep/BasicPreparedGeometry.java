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

import java.util.Iterator;
import java.util.List;

import com.vividsolutions.jts.algorithm.locate.*;
import com.vividsolutions.jts.algorithm.*;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.geom.util.ComponentCoordinateExtracter;

/**
 * A base class for {@link PreparedGeometry} subclasses.
 * Contains default implementations for methods, which simply delegate
 * to the equivalent {@link Geometry} methods.
 * This class may be used as a "no-op" class for Geometry types
 * which do not have a corresponding {@link PreparedGeometry} implementation.
 * 
 * @author Martin Davis
 *
 */
class BasicPreparedGeometry 
  implements PreparedGeometry
{
  private final Geometry baseGeom;
  private final List representativePts;  // List<Coordinate>

  public BasicPreparedGeometry(Geometry geom) 
  {
    baseGeom = geom;
    representativePts = ComponentCoordinateExtracter.getCoordinates(geom);
  }

  public Geometry getGeometry() { return baseGeom; }

  /**
   * Gets the list of representative points for this geometry.
   * One vertex is included for every component of the geometry
   * (i.e. including one for every ring of polygonal geometries).
   * 
   * Do not modify the returned list!
   * 
   * @return a List of Coordinate
   */
  public List getRepresentativePoints()
  {
	//TODO wrap in unmodifiable?
    return representativePts;
  }
  
	/**
	 * Tests whether any representative of the target geometry 
	 * intersects the test geometry.
	 * This is useful in A/A, A/L, A/P, L/P, and P/P cases.
	 * 
	 * @param geom the test geometry
	 * @param repPts the representative points of the target geometry
	 * @return true if any component intersects the areal test geometry
	 */
	public boolean isAnyTargetComponentInTest(Geometry testGeom)
	{
		PointLocator locator = new PointLocator();
    for (Iterator i = representativePts.iterator(); i.hasNext(); ) {
      Coordinate p = (Coordinate) i.next();
      if (locator.intersects(p, testGeom))
        return true;
    }
		return false;
	}

  /**
   * Determines whether a Geometry g interacts with 
   * this geometry by testing the geometry envelopes.
   *  
   * @param g a Geometry
   * @return true if the envelopes intersect
   */
  protected boolean envelopesIntersect(Geometry g)
  {
    if (! baseGeom.getEnvelopeInternal().intersects(g.getEnvelopeInternal()))
      return false;
    return true;
  }
  
  /**
   * Determines whether the envelope of 
   * this geometry covers the Geometry g.
   * 
   *  
   * @param g a Geometry
   * @return true if g is contained in this envelope
   */
  protected boolean envelopeCovers(Geometry g)
  {
    if (! baseGeom.getEnvelopeInternal().covers(g.getEnvelopeInternal()))
      return false;
    return true;
  }
  
  /**
   * Default implementation.
   */
  public boolean contains(Geometry g)
  {
    return baseGeom.contains(g);
  }

  /**
   * Default implementation.
   */
  public boolean containsProperly(Geometry g)
  {
  	// since raw relate is used, provide some optimizations
  	
    // short-circuit test
    if (! baseGeom.getEnvelopeInternal().contains(g.getEnvelopeInternal()))
      return false;
  	
    // otherwise, compute using relate mask
    return baseGeom.relate(g, "T**FF*FF*");
  }

  /**
   * Default implementation.
   */
  public boolean coveredBy(Geometry g)
  {
    return baseGeom.coveredBy(g);
  }

  /**
   * Default implementation.
   */
  public boolean covers(Geometry g)
  {
    return baseGeom.covers(g);
  }

  /**
   * Default implementation.
   */
  public boolean crosses(Geometry g)
  {
    return baseGeom.crosses(g);
  }
  
  /**
   * Standard implementation for all geometries.
   * Supports {@link GeometryCollection}s as input.
   */
  public boolean disjoint(Geometry g)
  {
    return ! intersects(g);
  }
  
  /**
   * Default implementation.
   */
  public boolean intersects(Geometry g)
  {
    return baseGeom.intersects(g);
  }
  
  /**
   * Default implementation.
   */
  public boolean overlaps(Geometry g)
  {
    return baseGeom.overlaps(g);
  }
  
  /**
   * Default implementation.
   */
  public boolean touches(Geometry g)
  {
    return baseGeom.touches(g);
  }
  
  /**
   * Default implementation.
   */
  public boolean within(Geometry g)
  {
    return baseGeom.within(g);
  }
  
  public String toString()
  {
  	return baseGeom.toString();
  }
}
