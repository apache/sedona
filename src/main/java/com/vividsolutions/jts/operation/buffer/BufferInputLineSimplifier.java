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
package com.vividsolutions.jts.operation.buffer;

import com.vividsolutions.jts.algorithm.CGAlgorithms;
import com.vividsolutions.jts.geom.*;

/**
 * Simplifies a buffer input line to 
 * remove concavities with shallow depth.
 * <p>
 * The most important benefit of doing this
 * is to reduce the number of points and the complexity of
 * shape which will be buffered.
 * It also reduces the risk of gores created by
 * the quantized fillet arcs (although this issue
 * should be eliminated in any case by the 
 * offset curve generation logic).
 * <p>
 * A key aspect of the simplification is that it
 * affects inside (concave or inward) corners only.  
 * Convex (outward) corners are preserved, since they
 * are required to ensure that the generated buffer curve
 * lies at the correct distance from the input geometry.
 * <p>
 * Another important heuristic used is that the end segments
 * of the input are never simplified.  This ensures that
 * the client buffer code is able to generate end caps faithfully.
 * <p>
 * No attempt is made to avoid self-intersections in the output.
 * This is acceptable for use for generating a buffer offset curve,
 * since the buffer algorithm is insensitive to invalid polygonal
 * geometry.  However, 
 * this means that this algorithm
 * cannot be used as a general-purpose polygon simplification technique.
 * 
 * @author Martin Davis
 *
 */
public class BufferInputLineSimplifier 
{
  /**
   * Simplify the input coordinate list.
   * If the distance tolerance is positive, 
   * concavities on the LEFT side of the line are simplified.
   * If the supplied distance tolerance is negative,
   * concavities on the RIGHT side of the line are simplified.
   * 
   * @param inputLine the coordinate list to simplify
   * @param distanceTol simplification distance tolerance to use
   * @return the simplified coordinate list
   */
  public static Coordinate[] simplify(Coordinate[] inputLine, double distanceTol)
  {
    BufferInputLineSimplifier simp = new BufferInputLineSimplifier(inputLine);
    return simp.simplify(distanceTol);
  }
  
  private static final int INIT = 0;
  private static final int DELETE = 1;
  private static final int KEEP = 1;
  
  
  private Coordinate[] inputLine;
  private double distanceTol;
  private byte[] isDeleted;
  private int angleOrientation = CGAlgorithms.COUNTERCLOCKWISE;
  
  public BufferInputLineSimplifier(Coordinate[] inputLine) {
    this.inputLine = inputLine;
  }

  /**
   * Simplify the input coordinate list.
   * If the distance tolerance is positive, 
   * concavities on the LEFT side of the line are simplified.
   * If the supplied distance tolerance is negative,
   * concavities on the RIGHT side of the line are simplified.
   * 
   * @param distanceTol simplification distance tolerance to use
   * @return the simplified coordinate list
   */
  public Coordinate[] simplify(double distanceTol)
  {
    this.distanceTol = Math.abs(distanceTol);
    if (distanceTol < 0)
      angleOrientation = CGAlgorithms.CLOCKWISE;
    
    // rely on fact that boolean array is filled with false value
    isDeleted = new byte[inputLine.length];
    
    boolean isChanged = false;
    do {
      isChanged = deleteShallowConcavities();
    } while (isChanged);
    
    return collapseLine();
  }
  
  /**
   * Uses a sliding window containing 3 vertices to detect shallow angles
   * in which the middle vertex can be deleted, since it does not
   * affect the shape of the resulting buffer in a significant way.
   * @return
   */
  private boolean deleteShallowConcavities()
  {
    /**
     * Do not simplify end line segments of the line string.
     * This ensures that end caps are generated consistently.
     */
    int index = 1;
    int maxIndex = inputLine.length - 1;
    
    int midIndex = findNextNonDeletedIndex(index);
    int lastIndex = findNextNonDeletedIndex(midIndex);
    
    boolean isChanged = false;
    while (lastIndex < inputLine.length) {
      // test triple for shallow concavity
    	boolean isMiddleVertexDeleted = false;
      if (isDeletable(index, midIndex, lastIndex, 
          distanceTol)) {
        isDeleted[midIndex] = DELETE;
        isMiddleVertexDeleted = true;
        isChanged = true;
      }
      // move simplification window forward
      if (isMiddleVertexDeleted)
      	index = lastIndex;
      else 
      	index = midIndex;
      
      midIndex = findNextNonDeletedIndex(index);
      lastIndex = findNextNonDeletedIndex(midIndex);
    }
    return isChanged;
  }
  
  /**
   * Finds the next non-deleted index, or the end of the point array if none
   * @param index
   * @return the next non-deleted index, if any
   * or inputLine.length if there are no more non-deleted indices
   */
  private int findNextNonDeletedIndex(int index)
  {
    int next = index + 1;
    while (next < inputLine.length && isDeleted[next] == DELETE)
      next++;
    return next;  
  }
  
  private Coordinate[] collapseLine()
  {
    CoordinateList coordList = new CoordinateList();
    for (int i = 0; i < inputLine.length; i++) {
      if (isDeleted[i] != DELETE)
        coordList.add(inputLine[i]);
    }
//    if (coordList.size() < inputLine.length)      System.out.println("Simplified " + (inputLine.length - coordList.size()) + " pts");
    return coordList.toCoordinateArray();
  }
  
  private boolean isDeletable(int i0, int i1, int i2, double distanceTol)
  {
  	Coordinate p0 = inputLine[i0];
  	Coordinate p1 = inputLine[i1];
  	Coordinate p2 = inputLine[i2];
  	
  	if (! isConcave(p0, p1, p2)) return false;
  	if (! isShallow(p0, p1, p2, distanceTol)) return false;
  	
  	// MD - don't use this heuristic - it's too restricting 
//  	if (p0.distance(p2) > distanceTol) return false;
  	
  	return isShallowSampled(p0, p1, i0, i2, distanceTol);
  }

  private boolean isShallowConcavity(Coordinate p0, Coordinate p1, Coordinate p2, double distanceTol)
  {
    int orientation = CGAlgorithms.computeOrientation(p0, p1, p2);
    boolean isAngleToSimplify = (orientation == angleOrientation);
    if (! isAngleToSimplify)
      return false;
    
    double dist = CGAlgorithms.distancePointLine(p1, p0, p2);
    return dist < distanceTol;
  }
  
  private static final int NUM_PTS_TO_CHECK = 10;
  
  /**
   * Checks for shallowness over a sample of points in the given section.
   * This helps prevents the siplification from incrementally  
   * "skipping" over points which are in fact non-shallow.
   * 
   * @param p0 start coordinate of section
   * @param p2 end coordinate of section
   * @param i0 start index of section
   * @param i2 end index of section
   * @param distanceTol distance tolerance
   * @return
   */
  private boolean isShallowSampled(Coordinate p0, Coordinate p2, int i0, int i2, double distanceTol)
  {
    // check every n'th point to see if it is within tolerance
  	int inc = (i2 - i0) / NUM_PTS_TO_CHECK;
  	if (inc <= 0) inc = 1;
  	
  	for (int i = i0; i < i2; i += inc) {
  		if (! isShallow(p0, p2, inputLine[i], distanceTol)) return false;
  	}
  	return true;
  }
  
  private boolean isShallow(Coordinate p0, Coordinate p1, Coordinate p2, double distanceTol)
  {
    double dist = CGAlgorithms.distancePointLine(p1, p0, p2);
    return dist < distanceTol;
  }
  
  
  private boolean isConcave(Coordinate p0, Coordinate p1, Coordinate p2)
  {
    int orientation = CGAlgorithms.computeOrientation(p0, p1, p2);
    boolean isConcave = (orientation == angleOrientation);
    return isConcave;
  }
}
