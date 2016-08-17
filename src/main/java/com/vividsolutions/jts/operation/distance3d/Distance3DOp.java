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

import com.vividsolutions.jts.algorithm.CGAlgorithms3D;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.LineSegment;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.operation.distance.GeometryLocation;

/**
 * Find two points on two 3D {@link Geometry}s which lie within a given distance,
 * or else are the nearest points on the geometries (in which case this also
 * provides the distance between the geometries).
 * <p>
 * 3D geometries have vertex Z ordinates defined.
 * 3D {@link Polygon}s are assumed to lie in a single plane (which is enforced if not actually the case).
 * 3D {@link LineString}s and {link Point}s may have any configuration.
 * <p>
 * The distance computation also finds a pair of points in the input geometries
 * which have the minimum distance between them. If a point lies in the interior
 * of a line segment, the coordinate computed is a close approximation to the
 * exact point.
 * <p>
 * The algorithms used are straightforward O(n^2) comparisons. This worst-case
 * performance could be improved on by using Voronoi techniques or spatial
 * indexes.
 * 
 * @version 1.7
 */
public class Distance3DOp {
	/**
	 * Compute the distance between the nearest points of two geometries.
	 * 
	 * @param g0
	 *            a {@link Geometry}
	 * @param g1
	 *            another {@link Geometry}
	 * @return the distance between the geometries
	 */
	public static double distance(Geometry g0, Geometry g1) {
		Distance3DOp distOp = new Distance3DOp(g0, g1);
		return distOp.distance();
	}

	/**
	 * Test whether two geometries lie within a given distance of each other.
	 * 
	 * @param g0
	 *            a {@link Geometry}
	 * @param g1
	 *            another {@link Geometry}
	 * @param distance
	 *            the distance to test
	 * @return true if g0.distance(g1) <= distance
	 */
	public static boolean isWithinDistance(Geometry g0, Geometry g1,
			double distance) {
		Distance3DOp distOp = new Distance3DOp(g0, g1, distance);
		return distOp.distance() <= distance;
	}

	/**
	 * Compute the the nearest points of two geometries. The points are
	 * presented in the same order as the input Geometries.
	 * 
	 * @param g0
	 *            a {@link Geometry}
	 * @param g1
	 *            another {@link Geometry}
	 * @return the nearest points in the geometries
	 */
	public static Coordinate[] nearestPoints(Geometry g0, Geometry g1) {
		Distance3DOp distOp = new Distance3DOp(g0, g1);
		return distOp.nearestPoints();
	}

	// input
	private Geometry[] geom;
	private double terminateDistance = 0.0;
	// working
	private GeometryLocation[] minDistanceLocation;
	private double minDistance = Double.MAX_VALUE;
	private boolean isDone = false;

	/**
	 * Constructs a DistanceOp that computes the distance and nearest points
	 * between the two specified geometries.
	 * 
	 * @param g0
	 *            a Geometry
	 * @param g1
	 *            a Geometry
	 */
	public Distance3DOp(Geometry g0, Geometry g1) {
		this(g0, g1, 0.0);
	}

	/**
	 * Constructs a DistanceOp that computes the distance and nearest points
	 * between the two specified geometries.
	 * 
	 * @param g0
	 *            a Geometry
	 * @param g1
	 *            a Geometry
	 * @param terminateDistance
	 *            the distance on which to terminate the search
	 */
	public Distance3DOp(Geometry g0, Geometry g1, double terminateDistance) {
		this.geom = new Geometry[2];
		geom[0] = g0;
		geom[1] = g1;
		this.terminateDistance = terminateDistance;
	}

	/**
	 * Report the distance between the nearest points on the input geometries.
	 * 
	 * @return the distance between the geometries, or 0 if either input geometry is empty
	 * @throws IllegalArgumentException
	 *             if either input geometry is null
	 */
	public double distance() {
		if (geom[0] == null || geom[1] == null)
			throw new IllegalArgumentException(
					"null geometries are not supported");
		if (geom[0].isEmpty() || geom[1].isEmpty())
			return 0.0;

		computeMinDistance();
		return minDistance;
	}

	/**
	 * Report the coordinates of the nearest points in the input geometries. The
	 * points are presented in the same order as the input Geometries.
	 * 
	 * @return a pair of {@link Coordinate}s of the nearest points
	 */
	public Coordinate[] nearestPoints() {
		computeMinDistance();
		Coordinate[] nearestPts = new Coordinate[] {
				minDistanceLocation[0].getCoordinate(),
				minDistanceLocation[1].getCoordinate() };
		return nearestPts;
	}

	/**
	 * Report the locations of the nearest points in the input geometries. The
	 * locations are presented in the same order as the input Geometries.
	 * 
	 * @return a pair of {@link GeometryLocation}s for the nearest points
	 */
	public GeometryLocation[] nearestLocations() {
		computeMinDistance();
		return minDistanceLocation;
	}

	private void updateDistance(double dist,
			GeometryLocation loc0, GeometryLocation loc1,
			boolean flip) {
		this.minDistance = dist;
		int index = flip ? 1 : 0;
		minDistanceLocation[index] = loc0;
		minDistanceLocation[1-index] = loc1;
		if (minDistance < terminateDistance)
			isDone = true;
	}

	private void computeMinDistance() {
		// only compute once
		if (minDistanceLocation != null)
			return;
		minDistanceLocation = new GeometryLocation[2];
		
		int geomIndex = mostPolygonalIndex();
		boolean flip = geomIndex == 0;
		computeMinDistanceMultiMulti(geom[geomIndex], geom[1-geomIndex], flip);
	}

	/**
	 * Finds the index of the "most polygonal" input geometry.
	 * This optimizes the computation of the best-fit plane, 
	 * since it is cached only for the left-hand geometry.
	 * 
	 * @return the index of the most polygonal geometry
	 */
	private int mostPolygonalIndex() {
		int dim0 = geom[0].getDimension();
		int dim1 = geom[1].getDimension();
		if (dim0 >= 2 && dim1 >= 2) {
			if (geom[0].getNumPoints() > geom[1].getNumPoints())
				return 0;
			return 1;
		}
		// no more than one is dim 2
		if (dim0 >= 2) return 0;
		if (dim1 >= 2) return 1;
		// both dim <= 1 - don't flip
		return 0;
	}

	private void computeMinDistanceMultiMulti(Geometry g0, Geometry g1, boolean flip) {
		if (g0 instanceof GeometryCollection) {
			int n = g0.getNumGeometries();
			for (int i = 0; i < n; i++) {
				Geometry g = g0.getGeometryN(i);
				computeMinDistanceMultiMulti(g, g1, flip);
				if (isDone)	return;
			}
		}
		else {
			// handle case of multigeom component being empty
			if (g0.isEmpty())
				return;
			
			// compute planar polygon only once for efficiency
			if (g0 instanceof Polygon) {
				computeMinDistanceOneMulti(polyPlane(g0), g1, flip);
			}
			else 
				computeMinDistanceOneMulti(g0, g1, flip);
		}
	}
	
	private void computeMinDistanceOneMulti(Geometry g0, Geometry g1, boolean flip) {
		if (g1 instanceof GeometryCollection) {
			int n = g1.getNumGeometries();
			for (int i = 0; i < n; i++) {
				Geometry g = g1.getGeometryN(i);
				computeMinDistanceOneMulti(g0, g, flip);
				if (isDone)	return;
			}
		}
		else {
			computeMinDistance(g0, g1, flip);
		}
	}

	private void computeMinDistanceOneMulti(PlanarPolygon3D poly, Geometry geom, boolean flip) {
		if (geom instanceof GeometryCollection) {
			int n = geom.getNumGeometries();
			for (int i = 0; i < n; i++) {
				Geometry g = geom.getGeometryN(i);
				computeMinDistanceOneMulti(poly, g, flip);
				if (isDone)	return;
			}
		}
		else {
			if (geom instanceof Point) {
				computeMinDistancePolygonPoint(poly, (Point) geom, flip);
				return;
			}
			if (geom instanceof LineString) {
				computeMinDistancePolygonLine(poly, (LineString) geom, flip);
				return;
			}
			if (geom instanceof Polygon) {
				computeMinDistancePolygonPolygon(poly, (Polygon) geom, flip);
				return;
			}
		}
	}

	/**
	 * Convenience method to create a Plane3DPolygon
	 * @param poly
	 * @return
	 */
	private static PlanarPolygon3D polyPlane(Geometry poly)
	{
		return new PlanarPolygon3D((Polygon) poly);
	}
	
	private void computeMinDistance(Geometry g0, Geometry g1, boolean flip) {
		if (g0 instanceof Point) {
			if (g1 instanceof Point) {
				computeMinDistancePointPoint((Point) g0, (Point) g1, flip);
				return;
			}
			if (g1 instanceof LineString) {
				computeMinDistanceLinePoint((LineString) g1, (Point) g0, ! flip);
				return;
			}
			if (g1 instanceof Polygon) {
				computeMinDistancePolygonPoint(polyPlane(g1), (Point) g0, ! flip);
				return;
			}
		}
		if (g0 instanceof LineString) {
			if (g1 instanceof Point) {
				computeMinDistanceLinePoint((LineString) g0, (Point) g1, flip);
				return;
			}
			if (g1 instanceof LineString) {
				computeMinDistanceLineLine((LineString) g0, (LineString) g1, flip);
				return;
			}
			if (g1 instanceof Polygon) {
				computeMinDistancePolygonLine(polyPlane(g1), (LineString) g0, ! flip);
				return;
			}
		}
		if (g0 instanceof Polygon) {
			if (g1 instanceof Point) {
				computeMinDistancePolygonPoint(polyPlane(g0), (Point) g1, flip);
				return;
			}
			if (g1 instanceof LineString) {
				computeMinDistancePolygonLine(polyPlane(g0), (LineString) g1, flip);
				return;
			}
			if (g1 instanceof Polygon) {
				computeMinDistancePolygonPolygon(polyPlane(g0), (Polygon) g1, flip);
				return;
			}
		}
	}

	/**
	 * Computes distance between two polygons.
	 * 
	 * To compute the distance, compute the distance
	 * between the rings of one polygon and the other polygon,
	 * and vice-versa.
	 * If the polygons intersect, then at least one ring must
	 * intersect the other polygon.
	 * Note that it is NOT sufficient to test only the shell rings. 
	 * A counter-example is a "figure-8" polygon A 
	 * and a simple polygon B at right angles to A, with the ring of B
	 * passing through the holes of A.
	 * The polygons intersect,
	 * but A's shell does not intersect B, and B's shell does not intersect A.
	 *  
	 * @param poly0
	 * @param poly1
	 * @param geomIndex
	 */
	private void computeMinDistancePolygonPolygon(PlanarPolygon3D poly0, Polygon poly1,
			boolean flip) {
		computeMinDistancePolygonRings(poly0, poly1, flip);
		if (isDone) return;
		PlanarPolygon3D polyPlane1 = new PlanarPolygon3D(poly1);
		computeMinDistancePolygonRings(polyPlane1, poly0.getPolygon(), flip);
	}

	/**
	 * Compute distance between a polygon and the rings of another.
	 * 
	 * @param poly
	 * @param ringPoly
	 * @param geomIndex
	 */
	private void computeMinDistancePolygonRings(PlanarPolygon3D poly, Polygon ringPoly,
			boolean flip) {
		// compute shell ring
		computeMinDistancePolygonLine(poly, ringPoly.getExteriorRing(), flip);
		if (isDone) return;
		// compute hole rings
		int nHole = ringPoly.getNumInteriorRing();
		for (int i = 0; i < nHole; i++) {
			computeMinDistancePolygonLine(poly, ringPoly.getInteriorRingN(i), flip);
			if (isDone) return;
		}
	}

	private void computeMinDistancePolygonLine(PlanarPolygon3D poly,LineString line, 
			boolean flip) {
		
		// first test if line intersects polygon
		Coordinate intPt = intersection(poly, line);
		if (intPt != null) {
			updateDistance(0,
					new GeometryLocation(poly.getPolygon(), 0, intPt),
					new GeometryLocation(line, 0, intPt),
					flip
			);
			return;
		}
		
		// if no intersection, then compute line distance to polygon rings
		computeMinDistanceLineLine(poly.getPolygon().getExteriorRing(), line, flip);
		if (isDone) return;
		int nHole = poly.getPolygon().getNumInteriorRing();
		for (int i = 0; i < nHole; i++) {
			computeMinDistanceLineLine(poly.getPolygon().getInteriorRingN(i), line, flip);
			if (isDone) return;
		}
	}

	private Coordinate intersection(PlanarPolygon3D poly,LineString line) {
		CoordinateSequence seq = line.getCoordinateSequence();
		if (seq.size() == 0)
			return null;

		// start point of line
		Coordinate p0 = new Coordinate();
		seq.getCoordinate(0, p0);
		double d0 = poly.getPlane().orientedDistance(p0);
		
		// for each segment in the line
		Coordinate p1 = new Coordinate();
		for (int i = 0; i < seq.size() - 1; i++) {
			seq.getCoordinate(i, p0);
			seq.getCoordinate(i + 1, p1);
			double d1 = poly.getPlane().orientedDistance(p1);

			/**
			 * If the oriented distances of the segment endpoints have the same sign, 
			 * the segment does not cross the plane, and is skipped.
			 */
			if (d0 * d1 > 0)
				continue;

			/**
			 * Compute segment-plane intersection point
			 * which is then used for a point-in-polygon test.
			 * The endpoint distances to the plane d0 and d1 
			 * give the proportional distance of the intersection point 
			 * along the segment.
			 */
			Coordinate intPt = segmentPoint(p0, p1, d0, d1);
			// Coordinate intPt = polyPlane.intersection(p0, p1, s0, s1);
			if (poly.intersects(intPt)) {
				return intPt;
			}

			// shift to next segment
			d0 = d1;
		}
		return null;
	}

	private void computeMinDistancePolygonPoint(PlanarPolygon3D polyPlane, Point point, 
			boolean flip) {
		Coordinate pt = point.getCoordinate();
		
		LineString shell = polyPlane.getPolygon().getExteriorRing();
		if (polyPlane.intersects(pt, shell)) {
			// point is either inside or in a hole
			
			int nHole = polyPlane.getPolygon().getNumInteriorRing();
			for (int i = 0; i < nHole; i++) {
				LineString hole = polyPlane.getPolygon().getInteriorRingN(i);
				if (polyPlane.intersects(pt, hole)) {
					computeMinDistanceLinePoint(hole, point, flip);
					return;
				}
			}
			// point is in interior of polygon
			// distance is distance to polygon plane
			double dist = Math.abs(polyPlane.getPlane().orientedDistance(pt));
			updateDistance(dist,
					new GeometryLocation(polyPlane.getPolygon(), 0, pt),
					new GeometryLocation(point, 0, pt),
					flip
			);
		}
		// point is outside polygon, so compute distance to shell linework
		computeMinDistanceLinePoint(shell, point, flip);
	}

	private void computeMinDistanceLineLine(LineString line0, LineString line1,
			boolean flip) {
		Coordinate[] coord0 = line0.getCoordinates();
		Coordinate[] coord1 = line1.getCoordinates();
		// brute force approach!
		for (int i = 0; i < coord0.length - 1; i++) {
			for (int j = 0; j < coord1.length - 1; j++) {
				double dist = CGAlgorithms3D.distanceSegmentSegment(coord0[i],
						coord0[i + 1], coord1[j], coord1[j + 1]);
				if (dist < minDistance) {
					minDistance = dist;
					// TODO: compute closest pts in 3D
					LineSegment seg0 = new LineSegment(coord0[i], coord0[i + 1]);
					LineSegment seg1 = new LineSegment(coord1[j], coord1[j + 1]);
					Coordinate[] closestPt = seg0.closestPoints(seg1);
					updateDistance(dist,
							new GeometryLocation(line0, i, closestPt[0]),
							new GeometryLocation(line1, j, closestPt[1]),
							flip
					);
				}
				if (isDone)	return;
			}
		}
	}

	private void computeMinDistanceLinePoint(LineString line,Point point, 
			boolean flip) {
		Coordinate[] lineCoord = line.getCoordinates();
		Coordinate coord = point.getCoordinate();
		// brute force approach!
		for (int i = 0; i < lineCoord.length - 1; i++) {
			double dist = CGAlgorithms3D.distancePointSegment(coord, lineCoord[i],
					lineCoord[i + 1]);
			if (dist < minDistance) {
				LineSegment seg = new LineSegment(lineCoord[i], lineCoord[i + 1]);
				Coordinate segClosestPoint = seg.closestPoint(coord);
				updateDistance(dist,
						new GeometryLocation(line, i, segClosestPoint),
						new GeometryLocation(point, 0, coord),
						flip);
			}
			if (isDone)	return;
		}
	}

	private void computeMinDistancePointPoint(Point point0, Point point1, boolean flip) {
		double dist = CGAlgorithms3D.distance(
				point0.getCoordinate(),
				point1.getCoordinate());
		if (dist < minDistance) {
			updateDistance(dist,
					new GeometryLocation(point0, 0,	point0.getCoordinate()),
					new GeometryLocation(point1, 0,	point1.getCoordinate()),
					flip);
		}
	}

	/**
	 * Computes a point at a distance along a segment
	 * specified by two relatively proportional values. 
	 * The fractional distance along the segment is d0/(d0+d1).
	 * 
	 * @param p0
	 *            start point of the segment
	 * @param p1
	 *            end point of the segment
	 * @param d0
	 *            proportional distance from start point to computed point
	 * @param d1
	 *            proportional distance from computed point to end point
	 * @return the computed point
	 */
	private static Coordinate segmentPoint(Coordinate p0, Coordinate p1, double d0,
			double d1) {
		if (d0 <= 0) return new Coordinate(p0);
		if (d1 <= 0) return new Coordinate(p1);
		
		double f = Math.abs(d0) / (Math.abs(d0) + Math.abs(d1));
		double intx = p0.x + f * (p1.x - p0.x);
		double inty = p0.y + f * (p1.y - p0.y);
		double intz = p0.z + f * (p1.z - p0.z);
		return new Coordinate(intx, inty, intz);
	}




}
