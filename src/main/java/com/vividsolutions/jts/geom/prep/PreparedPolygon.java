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


import com.vividsolutions.jts.algorithm.locate.*;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.noding.*;
import com.vividsolutions.jts.operation.predicate.*;

/**
 * A prepared version for {@link Polygonal} geometries.
 * This class supports both {@link Polygon}s and {@link MultiPolygon}s.
 * <p>
 * This class does <b>not</b> support MultiPolygons which are non-valid 
 * (e.g. with overlapping elements). 
 * <p>
 * Instances of this class are thread-safe and immutable.
 * 
 * @author mbdavis
 *
 */
public class PreparedPolygon
  extends BasicPreparedGeometry
{
	private final boolean isRectangle;
	// create these lazily, since they are expensive
	private FastSegmentSetIntersectionFinder segIntFinder = null;
	private PointOnGeometryLocator pia = null;

  public PreparedPolygon(Polygonal poly) {
    super((Geometry) poly);
    isRectangle = getGeometry().isRectangle();
  }

  /**
   * Gets the indexed intersection finder for this geometry.
   * 
   * @return the intersection finder
   */
  public synchronized FastSegmentSetIntersectionFinder getIntersectionFinder()
  {
  	/**
  	 * MD - Another option would be to use a simple scan for 
  	 * segment testing for small geometries.  
  	 * However, testing indicates that there is no particular advantage 
  	 * to this approach.
  	 */
  	if (segIntFinder == null)
  		segIntFinder = new FastSegmentSetIntersectionFinder(SegmentStringUtil.extractSegmentStrings(getGeometry()));
  	return segIntFinder;
  }
  
  public synchronized PointOnGeometryLocator getPointLocator()
  {
  	if (pia == null)
      pia = new IndexedPointInAreaLocator(getGeometry());
 		
    return pia;
  }
  
  public boolean intersects(Geometry g)
  {
  	// envelope test
  	if (! envelopesIntersect(g)) return false;
  	
    // optimization for rectangles
    if (isRectangle) {
      return RectangleIntersects.intersects((Polygon) getGeometry(), g);
    }
    
    return PreparedPolygonIntersects.intersects(this, g);
  }
  
  public boolean contains(Geometry g)
  {
    // short-circuit test
    if (! envelopeCovers(g)) 
    	return false;
  	
    // optimization for rectangles
    if (isRectangle) {
      return RectangleContains.contains((Polygon) getGeometry(), g);
    }

    return PreparedPolygonContains.contains(this, g);
  }
  
  public boolean containsProperly(Geometry g)
  {
    // short-circuit test
    if (! envelopeCovers(g)) 
    	return false;
    return PreparedPolygonContainsProperly.containsProperly(this, g);
  }
  
  public boolean covers(Geometry g)
  {
    // short-circuit test
    if (! envelopeCovers(g)) 
    	return false;
    // optimization for rectangle arguments
    if (isRectangle) {
      return true;
    }
    return PreparedPolygonCovers.covers(this, g);
  }
}
