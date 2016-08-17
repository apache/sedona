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

import com.vividsolutions.jts.util.StringUtil;


/**
 * Utility functions for manipulating {@link CoordinateSequence}s
 *
 * @version 1.7
 */
public class CoordinateSequences {

  /**
   * Reverses the coordinates in a sequence in-place.
   */
  public static void reverse(CoordinateSequence seq)
  {
    int last = seq.size() - 1;
    int mid = last / 2;
    for (int i = 0; i <= mid; i++) {
      swap(seq, i, last - i);
    }
  }

  /**
   * Swaps two coordinates in a sequence.
   *
   * @param seq the sequence to modify
   * @param i the index of a coordinate to swap
   * @param j the index of a coordinate to swap
   */
  public static void swap(CoordinateSequence seq, int i, int j)
  {
    if (i == j) return;
    for (int dim = 0; dim < seq.getDimension(); dim++) {
      double tmp = seq.getOrdinate(i, dim);
      seq.setOrdinate(i, dim, seq.getOrdinate(j, dim));
      seq.setOrdinate(j, dim, tmp);
    }
  }
  
  /**
   * Copies a section of a {@link CoordinateSequence} to another {@link CoordinateSequence}.
   * The sequences may have different dimensions;
   * in this case only the common dimensions are copied.
   *
   * @param src the sequence to copy from
   * @param srcPos the position in the source sequence to start copying at
   * @param dest the sequence to copy to
   * @param destPos the position in the destination sequence to copy to
   * @param length the number of coordinates to copy
   */
  public static void copy(CoordinateSequence src, int srcPos, CoordinateSequence dest, int destPos, int length)
  {
  	for (int i = 0; i < length; i++) {
  		copyCoord(src, srcPos + i, dest, destPos + i);
  	}
  }

  /**
   * Copies a coordinate of a {@link CoordinateSequence} to another {@link CoordinateSequence}.
   * The sequences may have different dimensions;
   * in this case only the common dimensions are copied.
   * 
   * @param src the sequence to copy from
   * @param srcPos the source coordinate to copy
   * @param dest the sequence to copy to
   * @param destPos the destination coordinate to copy to
   */
  public static void copyCoord(CoordinateSequence src, int srcPos, CoordinateSequence dest, int destPos)
  {
    int minDim = Math.min(src.getDimension(), dest.getDimension());
		for (int dim = 0; dim < minDim; dim++) {
			dest.setOrdinate(destPos, dim, src.getOrdinate(srcPos, dim));
		}
  }
  
  /**
   * Tests whether a {@link CoordinateSequence} forms a valid {@link LinearRing},
   * by checking the sequence length and closure
   * (whether the first and last points are identical in 2D). 
   * Self-intersection is not checked.
   * 
   * @param seq the sequence to test
   * @return true if the sequence is a ring
   * @see LinearRing
   */
  public static boolean isRing(CoordinateSequence seq) 
  {
  	int n = seq.size();
  	if (n == 0) return true;
  	// too few points
  	if (n <= 3) 
  		return false;
  	// test if closed
  	return seq.getOrdinate(0, CoordinateSequence.X) == seq.getOrdinate(n-1, CoordinateSequence.X)
  		&& seq.getOrdinate(0, CoordinateSequence.Y) == seq.getOrdinate(n-1, CoordinateSequence.Y);
  }
  
  /**
   * Ensures that a CoordinateSequence forms a valid ring, 
   * returning a new closed sequence of the correct length if required.
   * If the input sequence is already a valid ring, it is returned 
   * without modification.
   * If the input sequence is too short or is not closed, 
   * it is extended with one or more copies of the start point.
   * 
   * @param fact the CoordinateSequenceFactory to use to create the new sequence
   * @param seq the sequence to test
   * @return the original sequence, if it was a valid ring, or a new sequence which is valid.
   */
  public static CoordinateSequence ensureValidRing(CoordinateSequenceFactory fact, CoordinateSequence seq)
  {
  	int n = seq.size();
  	// empty sequence is valid
  	if (n == 0) return seq; 
  	// too short - make a new one
  	if (n <= 3) 
  		return createClosedRing(fact, seq, 4);
  	
  	boolean isClosed = seq.getOrdinate(0, CoordinateSequence.X) == seq.getOrdinate(n-1, CoordinateSequence.X)
		&& seq.getOrdinate(0, CoordinateSequence.Y) == seq.getOrdinate(n-1, CoordinateSequence.Y);
  	if (isClosed) return seq;
  	// make a new closed ring
  	return createClosedRing(fact, seq, n+1);
  }
  
  private static CoordinateSequence createClosedRing(CoordinateSequenceFactory fact, CoordinateSequence seq, int size)
  {
    CoordinateSequence newseq = fact.create(size, seq.getDimension());
    int n = seq.size();
    copy(seq, 0, newseq, 0, n);
    // fill remaining coordinates with start point
    for (int i = n; i < size; i++)
      copy(seq, 0, newseq, i, 1);
    return newseq;
  }
  
  public static CoordinateSequence extend(CoordinateSequenceFactory fact, CoordinateSequence seq, int size)
  {
    CoordinateSequence newseq = fact.create(size, seq.getDimension());
    int n = seq.size();
    copy(seq, 0, newseq, 0, n);
    // fill remaining coordinates with end point, if it exists
    if (n > 0) {
      for (int i = n; i < size; i++)
        copy(seq, n-1, newseq, i, 1);
    }
    return newseq;
  }

  /**
   * Tests whether two {@link CoordinateSequence}s are equal.
   * To be equal, the sequences must be the same length.
   * They do not need to be of the same dimension, 
   * but the ordinate values for the smallest dimension of the two
   * must be equal.
   * Two <code>NaN</code> ordinates values are considered to be equal. 
   * 
   * @param cs1 a CoordinateSequence
   * @param cs2 a CoordinateSequence
   * @return true if the sequences are equal in the common dimensions
   */
  public static boolean isEqual(CoordinateSequence cs1, CoordinateSequence cs2) {
    int cs1Size = cs1.size();
    int cs2Size = cs2.size();
    if (cs1Size != cs2Size) return false;
    int dim = Math.min(cs1.getDimension(), cs2.getDimension());
    for (int i = 0; i < cs1Size; i++) {
      for (int d = 0; d < dim; d++) {
        double v1 = cs1.getOrdinate(i, d);
        double v2 = cs2.getOrdinate(i, d);
        if (cs1.getOrdinate(i, d) == cs2.getOrdinate(i, d))
          continue;
        // special check for NaNs
        if (Double.isNaN(v1) && Double.isNaN(v2))
          continue;
        return false;
      }
    }
    return true;
  }
  
  /**
   * Creates a string representation of a {@link CoordinateSequence}.
   * The format is:
   * <pre>
   *   ( ord0,ord1.. ord0,ord1,...  ... )
   * </pre>
   * 
   * @param cs the sequence to output
   * @return the string representation of the sequence
   */
  public static String toString(CoordinateSequence cs)
  {
    int size = cs.size();
    if (size == 0) 
      return "()";
    int dim = cs.getDimension();
    StringBuffer buf = new StringBuffer();
    buf.append('(');
    for (int i = 0; i < size; i++) {
      if (i > 0) buf.append(" ");
      for (int d = 0; d < dim; d++) {
        if (d > 0) buf.append(",");
        buf.append(StringUtil.toString(cs.getOrdinate(i, d)));
      }
    }
    buf.append(')');
    return buf.toString();
  }
}