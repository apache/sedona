
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
 * Extracts a single representative {@link Coordinate} 
 * from each connected component of a {@link Geometry}.
 *
 * @version 1.9
 */
public class ComponentCoordinateExtracter
  implements GeometryComponentFilter
{

  /**
   * Extracts the linear components from a single geometry.
   * If more than one geometry is to be processed, it is more
   * efficient to create a single {@link ComponentCoordinateExtracter} instance
   * and pass it to multiple geometries.
   *
   * @param geom the Geometry from which to extract
   * @return a list of Coordinates
   */
  public static List getCoordinates(Geometry geom)
  {
    List coords = new ArrayList();
    geom.apply(new ComponentCoordinateExtracter(coords));
    return coords;
  }

  private List coords;

  /**
   * Constructs a LineExtracterFilter with a list in which to store LineStrings found.
   */
  public ComponentCoordinateExtracter(List coords)
  {
    this.coords = coords;
  }

  public void filter(Geometry geom)
  {
    // add coordinates from connected components
    if (geom instanceof LineString
        || geom instanceof Point) 
      coords.add(geom.getCoordinate());
  }

}
