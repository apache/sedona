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

package com.vividsolutions.jts.operation.distance3d;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.Envelope;

/**
 * A CoordinateSequence wrapper which 
 * projects 3D coordinates into one of the
 * three Cartesian axis planes,
 * using the standard orthonormal projection
 * (i.e. simply selecting the appropriate ordinates into the XY ordinates).
 * The projected data is represented as 2D coordinates.
 * 
 * @author mdavis
 *
 */
public class AxisPlaneCoordinateSequence implements CoordinateSequence {

	/**
	 * Creates a wrapper projecting to the XY plane.
	 * 
	 * @param seq the sequence to be projected
	 * @return a sequence which projects coordinates
	 */
	public static CoordinateSequence projectToXY(CoordinateSequence seq)
	{
		/**
		 * This is just a no-op, but return a wrapper
		 * to allow better testing
		 */
		return new AxisPlaneCoordinateSequence(seq, XY_INDEX);
	}
	
	/**
	 * Creates a wrapper projecting to the XZ plane.
	 * 
	 * @param seq the sequence to be projected
	 * @return a sequence which projects coordinates
	 */
	public static CoordinateSequence projectToXZ(CoordinateSequence seq)
	{
		return new AxisPlaneCoordinateSequence(seq, XZ_INDEX);
	}
	
	/**
	 * Creates a wrapper projecting to the YZ plane.
	 * 
	 * @param seq the sequence to be projected
	 * @return a sequence which projects coordinates
	 */
	public static CoordinateSequence projectToYZ(CoordinateSequence seq)
	{
		return new AxisPlaneCoordinateSequence(seq, YZ_INDEX);
	}
	
	private static final int[] XY_INDEX = new int[] { 0,1 };
	private static final int[] XZ_INDEX = new int[] { 0,2 };
	private static final int[] YZ_INDEX = new int[] { 1,2 };
	
	private CoordinateSequence seq;
	private int[] indexMap;
	
	private AxisPlaneCoordinateSequence(CoordinateSequence seq, int[] indexMap) {
		this.seq = seq;
		this.indexMap = indexMap;
	}

	public int getDimension() {
		return 2;
	}

	public Coordinate getCoordinate(int i) {
		return getCoordinateCopy(i);
	}

	public Coordinate getCoordinateCopy(int i) {
		return new Coordinate(getX(i), getY(i), getZ(i));
	}

	public void getCoordinate(int index, Coordinate coord) {
		coord.x = getOrdinate(index, X);
		coord.y = getOrdinate(index, Y);
		coord.z = getOrdinate(index, Z);
	}

	public double getX(int index) {
		return getOrdinate(index, X);
	}

	public double getY(int index) {
		return getOrdinate(index, Y);
	}

	public double getZ(int index) {
		return getOrdinate(index, Z);
	}

	public double getOrdinate(int index, int ordinateIndex) {
		// Z ord is always 0
		if (ordinateIndex > 1) return 0;
		return seq.getOrdinate(index, indexMap[ordinateIndex]);
	}

	public int size() {
		return seq.size();
	}

	public void setOrdinate(int index, int ordinateIndex, double value) {
		throw new UnsupportedOperationException();
	}

	public Coordinate[] toCoordinateArray() {
		throw new UnsupportedOperationException();
	}

	public Envelope expandEnvelope(Envelope env) {
		throw new UnsupportedOperationException();
	}

	public Object clone()
	{
		throw new UnsupportedOperationException();		
	}

}
