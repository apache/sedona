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
 * The internal representation of a list of coordinates inside a Geometry.
 * <p>
 * This allows Geometries to store their
 * points using something other than the JTS {@link Coordinate} class. 
 * For example, a storage-efficient implementation
 * might store coordinate sequences as an array of x's
 * and an array of y's. 
 * Or a custom coordinate class might support extra attributes like M-values.
 * <p>
 * Implementing a custom coordinate storage structure
 * requires implementing the {@link CoordinateSequence} and
 * {@link CoordinateSequenceFactory} interfaces. 
 * To use the custom CoordinateSequence, create a
 * new {@link GeometryFactory} parameterized by the CoordinateSequenceFactory
 * The {@link GeometryFactory} can then be used to create new {@link Geometry}s.
 * The new Geometries
 * will use the custom CoordinateSequence implementation.
 * <p>
 * For an example, see the code for
 * {@link ExtendedCoordinateExample}.
 *
 * @see CoordinateArraySequenceFactory
 * @see PackedCoordinateSequenceFactory
 * @see ExtendedCoordinateExample
 *
 * @version 1.7
 */
public interface CoordinateSequence
    extends Cloneable
{
  /**
   * Standard ordinate index values
   */
  int X = 0;
  int Y = 1;
  int Z = 2;
  int M = 3;

  /**
   * Returns the dimension (number of ordinates in each coordinate)
   * for this sequence.
   *
   * @return the dimension of the sequence.
   */
  int getDimension();

  /**
   * Returns (possibly a copy of) the i'th coordinate in this sequence.
   * Whether or not the Coordinate returned is the actual underlying
   * Coordinate or merely a copy depends on the implementation.
   * <p>
   * Note that in the future the semantics of this method may change
   * to guarantee that the Coordinate returned is always a copy.
   * Callers should not to assume that they can modify a CoordinateSequence by
   * modifying the object returned by this method.
   *
   * @param i the index of the coordinate to retrieve
   * @return the i'th coordinate in the sequence
   */
  Coordinate getCoordinate(int i);

  /**
   * Returns a copy of the i'th coordinate in this sequence.
   * This method optimizes the situation where the caller is
   * going to make a copy anyway - if the implementation
   * has already created a new Coordinate object, no further copy is needed.
   *
   * @param i the index of the coordinate to retrieve
   * @return a copy of the i'th coordinate in the sequence
   */
  Coordinate getCoordinateCopy(int i);

  /**
   * Copies the i'th coordinate in the sequence to the supplied
   * {@link Coordinate}.  Only the first two dimensions are copied.
   *
   * @param index the index of the coordinate to copy
   * @param coord a {@link Coordinate} to receive the value
   */
  void getCoordinate(int index, Coordinate coord);

  /**
   * Returns ordinate X (0) of the specified coordinate.
   *
   * @param index
   * @return the value of the X ordinate in the index'th coordinate
   */
  double getX(int index);

  /**
   * Returns ordinate Y (1) of the specified coordinate.
   *
   * @param index
   * @return the value of the Y ordinate in the index'th coordinate
   */
  double getY(int index);

  /**
   * Returns the ordinate of a coordinate in this sequence.
   * Ordinate indices 0 and 1 are assumed to be X and Y.
   * Ordinates indices greater than 1 have user-defined semantics
   * (for instance, they may contain other dimensions or measure values).
   *
   * @param index  the coordinate index in the sequence
   * @param ordinateIndex the ordinate index in the coordinate (in range [0, dimension-1])
   */
  double getOrdinate(int index, int ordinateIndex);

  /**
   * Returns the number of coordinates in this sequence.
   * @return the size of the sequence
   */
  int size();

  /**
   * Sets the value for a given ordinate of a coordinate in this sequence.
   *
   * @param index  the coordinate index in the sequence
   * @param ordinateIndex the ordinate index in the coordinate (in range [0, dimension-1])
   * @param value  the new ordinate value
   */
  void setOrdinate(int index, int ordinateIndex, double value);

  /**
   * Returns (possibly copies of) the Coordinates in this collection.
   * Whether or not the Coordinates returned are the actual underlying
   * Coordinates or merely copies depends on the implementation. Note that
   * if this implementation does not store its data as an array of Coordinates,
   * this method will incur a performance penalty because the array needs to
   * be built from scratch.
   *
   * @return a array of coordinates containing the point values in this sequence
   */
  Coordinate[] toCoordinateArray();

  /**
   * Expands the given {@link Envelope} to include the coordinates in the sequence.
   * Allows implementing classes to optimize access to coordinate values.
   *
   * @param env the envelope to expand
   * @return a ref to the expanded envelope
   */
  Envelope expandEnvelope(Envelope env);

  /**
   * Returns a deep copy of this collection.
   * Called by Geometry#clone.
   *
   * @return a copy of the coordinate sequence containing copies of all points
   */
  Object clone();
}