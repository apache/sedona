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
package com.vividsolutions.jts.geom;

/**
 * A factory to create concrete instances of {@link CoordinateSequence}s.
 * Used to configure {@link GeometryFactory}s
 * to provide specific kinds of CoordinateSequences.
 *
 * @version 1.7
 */
public interface CoordinateSequenceFactory
{

  /**
   * Returns a {@link CoordinateSequence} based on the given array.
   * Whether the array is copied or simply referenced
   * is implementation-dependent.
   * This method must handle null arguments by creating an empty sequence.
   *
   * @param coordinates the coordinates
   */
  CoordinateSequence create(Coordinate[] coordinates);

  /**
   * Creates a {@link CoordinateSequence} which is a copy
   * of the given {@link CoordinateSequence}.
   * This method must handle null arguments by creating an empty sequence.
   *
   * @param coordSeq the coordinate sequence to copy
   */
  CoordinateSequence create(CoordinateSequence coordSeq);

  /**
   * Creates a {@link CoordinateSequence} of the specified size and dimension.
   * For this to be useful, the {@link CoordinateSequence} implementation must
   * be mutable.
   * <p>
   * If the requested dimension is larger than the CoordinateSequence implementation
   * can provide, then a sequence of maximum possible dimension should be created.
   * An error should not be thrown.
   *
   * @param size the number of coordinates in the sequence
   * @param dimension the dimension of the coordinates in the sequence (if user-specifiable,
   * otherwise ignored)
   */
  CoordinateSequence create(int size, int dimension);

}