
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
 * Extracts all the {@link LineString} elements from a {@link Geometry}.
 *
 * @version 1.7
 * @see GeometryExtracter
 */
public class LineStringExtracter
  implements GeometryFilter
{
  /**
   * Extracts the {@link LineString} elements from a single {@link Geometry}
   * and adds them to the provided {@link List}.
   * 
   * @param geom the geometry from which to extract
   * @param lines the list to add the extracted LineStrings to
   * @return the list argument
   */
  public static List getLines(Geometry geom, List lines)
  {
  	if (geom instanceof LineString) {
  		lines.add(geom);
  	}
  	else if (geom instanceof GeometryCollection) {
  		geom.apply(new LineStringExtracter(lines));
  	}
  	// skip non-LineString elemental geometries
  	
    return lines;
  }

  /**
   * Extracts the {@link LineString} elements from a single {@link Geometry}
   * and returns them in a {@link List}.
   * 
   * @param geom the geometry from which to extract
   * @return a list containing the linear elements
   */
  public static List getLines(Geometry geom)
  {
    return getLines(geom, new ArrayList());
  }

  /**
   * Extracts the {@link LineString} elements from a single {@link Geometry}
   * and returns them as either a {@link LineString) or {@link MultiLineString}.
   * 
   * @param geom the geometry from which to extract
   * @return a linear geometry
  */
  public static Geometry getGeometry(Geometry geom)
  {
    return geom.getFactory().buildGeometry(getLines(geom));
  }

  private List comps;
  
  /**
   * Constructs a filter with a list in which to store the elements found.
   */
  public LineStringExtracter(List comps)
  {
    this.comps = comps;
  }

  public void filter(Geometry geom)
  {
    if (geom instanceof LineString) comps.add(geom);
  }

}
