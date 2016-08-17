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
package com.vividsolutions.jts.geom.impl;

import java.io.Serializable;
import com.vividsolutions.jts.geom.*;

/**
 * Creates {@link CoordinateSequence}s represented as an array of {@link Coordinate}s.
 *
 * @version 1.7
 */
public final class CoordinateArraySequenceFactory
    implements CoordinateSequenceFactory, Serializable
{
  private static final long serialVersionUID = -4099577099607551657L;
  private static final CoordinateArraySequenceFactory instanceObject = new CoordinateArraySequenceFactory();

  private CoordinateArraySequenceFactory() {
  }

  private Object readResolve() {
  	// http://www.javaworld.com/javaworld/javatips/jw-javatip122.html
    return CoordinateArraySequenceFactory.instance();
  }

  /**
   * Returns the singleton instance of {@link CoordinateArraySequenceFactory}
   */
  public static CoordinateArraySequenceFactory instance() {
    return instanceObject;
  }

  /**
   * Returns a {@link CoordinateArraySequence} based on the given array (the array is
   * not copied).
   *
   * @param coordinates
   *            the coordinates, which may not be null nor contain null
   *            elements
   */
  public CoordinateSequence create(Coordinate[] coordinates) {
    return new CoordinateArraySequence(coordinates);
  }

  /**
   * @see com.vividsolutions.jts.geom.CoordinateSequenceFactory#create(com.vividsolutions.jts.geom.CoordinateSequence)
   */
  public CoordinateSequence create(CoordinateSequence coordSeq) {
    return new CoordinateArraySequence(coordSeq);
  }

  /**
   * The created sequence dimension is clamped to be <= 3. 
   * 
   * @see com.vividsolutions.jts.geom.CoordinateSequenceFactory#create(int, int)
   *
   */
  public CoordinateSequence create(int size, int dimension) {
    if (dimension > 3)
      dimension = 3;
      //throw new IllegalArgumentException("dimension must be <= 3");
    // handle bogus dimension
    if (dimension < 2)
    	// TODO: change to dimension = 2  ???
      return new CoordinateArraySequence(size);
    return new CoordinateArraySequence(size, dimension);
  }
}