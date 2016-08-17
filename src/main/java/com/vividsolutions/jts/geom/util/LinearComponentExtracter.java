
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
package com.vividsolutions.jts.geom.util;

import java.util.*;

import com.vividsolutions.jts.geom.*;

/**
 * Extracts all the 1-dimensional ({@link LineString}) components from a {@link Geometry}.
 * For polygonal geometries, this will extract all the component {@link LinearRing}s.
 * If desired, <code>LinearRing</code>s can be forced to be returned as <code>LineString</code>s.
 *
 * @version 1.7
 */
public class LinearComponentExtracter
  implements GeometryComponentFilter
{
  /**
   * Extracts the linear components from a single {@link Geometry}
   * and adds them to the provided {@link Collection}.
   *
   * @param geoms the collection of geometries from which to extract linear components
   * @param lines the collection to add the extracted linear components to
   * @return the collection of linear components (LineStrings or LinearRings)
   */
  public static Collection getLines(Collection geoms, Collection lines)
  {
  	for (Iterator i = geoms.iterator(); i.hasNext(); ) {
  		Geometry g = (Geometry) i.next();
  		getLines(g, lines);
  	}
    return lines;
  }

  /**
   * Extracts the linear components from a single {@link Geometry}
   * and adds them to the provided {@link Collection}.
   *
   * @param geoms the Collection of geometries from which to extract linear components
   * @param lines the collection to add the extracted linear components to
   * @param forceToLineString true if LinearRings should be converted to LineStrings
   * @return the collection of linear components (LineStrings or LinearRings)
   */
  public static Collection getLines(Collection geoms, Collection lines, boolean forceToLineString)
  {
  	for (Iterator i = geoms.iterator(); i.hasNext(); ) {
  		Geometry g = (Geometry) i.next();
  		getLines(g, lines, forceToLineString);
  	}
    return lines;
  }

  /**
   * Extracts the linear components from a single {@link Geometry}
   * and adds them to the provided {@link Collection}.
   *
   * @param geom the geometry from which to extract linear components
   * @param lines the Collection to add the extracted linear components to
   * @return the Collection of linear components (LineStrings or LinearRings)
   */
  public static Collection getLines(Geometry geom, Collection lines)
  {
  	if (geom instanceof LineString) {
  		lines.add(geom);
  	}
  	else {
      geom.apply(new LinearComponentExtracter(lines));
  	}
    return lines;
  }

  /**
   * Extracts the linear components from a single {@link Geometry}
   * and adds them to the provided {@link Collection}.
   *
   * @param geom the geometry from which to extract linear components
   * @param lines the Collection to add the extracted linear components to
   * @param forceToLineString true if LinearRings should be converted to LineStrings
   * @return the Collection of linear components (LineStrings or LinearRings)
   */
  public static Collection getLines(Geometry geom, Collection lines, boolean forceToLineString)
  {
    geom.apply(new LinearComponentExtracter(lines, forceToLineString));
    return lines;
  }

  /**
   * Extracts the linear components from a single geometry.
   * If more than one geometry is to be processed, it is more
   * efficient to create a single {@link LinearComponentExtracter} instance
   * and pass it to multiple geometries.
   *
   * @param geom the geometry from which to extract linear components
   * @return the list of linear components
   */
  public static List getLines(Geometry geom)
  {
    return getLines(geom, false);
  }

  /**
   * Extracts the linear components from a single geometry.
   * If more than one geometry is to be processed, it is more
   * efficient to create a single {@link LinearComponentExtracter} instance
   * and pass it to multiple geometries.
   *
   * @param geom the geometry from which to extract linear components
   * @param forceToLineString true if LinearRings should be converted to LineStrings
   * @return the list of linear components
   */
  public static List getLines(Geometry geom, boolean forceToLineString)
  {
    List lines = new ArrayList();
    geom.apply(new LinearComponentExtracter(lines, forceToLineString));
    return lines;
  }

  /**
   * Extracts the linear components from a single {@link Geometry}
   * and returns them as either a {@link LineString) or {@link MultiLineString}.
   * 
   * @param geom the geometry from which to extract
   * @return a linear geometry
   */
  public static Geometry getGeometry(Geometry geom)
  {
    return geom.getFactory().buildGeometry(getLines(geom));
  }


  /**
   * Extracts the linear components from a single {@link Geometry}
   * and returns them as either a {@link LineString) or {@link MultiLineString}.
   * 
   * @param geom the geometry from which to extract
   * @param forceToLineString true if LinearRings should be converted to LineStrings
   * @return a linear geometry
   */
  public static Geometry getGeometry(Geometry geom, boolean forceToLineString)
  {
    return geom.getFactory().buildGeometry(getLines(geom, forceToLineString));
  }


  private Collection lines;
  private boolean isForcedToLineString = false;
  
  /**
   * Constructs a LineExtracterFilter with a list in which to store LineStrings found.
   */
  public LinearComponentExtracter(Collection lines)
  {
    this.lines = lines;
  }

  /**
   * Constructs a LineExtracterFilter with a list in which to store LineStrings found.
   */
  public LinearComponentExtracter(Collection lines, boolean isForcedToLineString)
  {
    this.lines = lines;
    this.isForcedToLineString = isForcedToLineString;
  }

  /**
   * Indicates that LinearRing components should be 
   * converted to pure LineStrings.
   * 
   * @param isForcedToLineString true if LinearRings should be converted to LineStrings
   */
  public void setForceToLineString(boolean isForcedToLineString)
  {
  	this.isForcedToLineString = isForcedToLineString;
  }
  
  public void filter(Geometry geom)
  {
  	if (isForcedToLineString && geom instanceof LinearRing) {
  		LineString line = geom.getFactory().createLineString( ((LinearRing) geom).getCoordinateSequence());
  		lines.add(line);
  		return;
  	}
  	// if not being forced, and this is a linear component
  	if (geom instanceof LineString) 
  		lines.add(geom);
  	
  	// else this is not a linear component, so skip it
  }

}
