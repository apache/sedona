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

package com.vividsolutions.jts.algorithm.distance;

import com.vividsolutions.jts.geom.*;

/**
 * An algorithm for computing a distance metric
 * which is an approximation to the Hausdorff Distance
 * based on a discretization of the input {@link Geometry}.
 * The algorithm computes the Hausdorff distance restricted to discrete points
 * for one of the geometries.
 * The points can be either the vertices of the geometries (the default), 
 * or the geometries with line segments densified by a given fraction.
 * Also determines two points of the Geometries which are separated by the computed distance.
* <p>
 * This algorithm is an approximation to the standard Hausdorff distance.
 * Specifically, 
 * <pre>
 *    for all geometries a, b:    DHD(a, b) <= HD(a, b)
 * </pre>
 * The approximation can be made as close as needed by densifying the input geometries.  
 * In the limit, this value will approach the true Hausdorff distance:
 * <pre>
 *    DHD(A, B, densifyFactor) -> HD(A, B) as densifyFactor -> 0.0
 * </pre>
 * The default approximation is exact or close enough for a large subset of useful cases.
 * Examples of these are:
 * <ul>
 * <li>computing distance between Linestrings that are roughly parallel to each other,
 * and roughly equal in length.  This occurs in matching linear networks.
 * <li>Testing similarity of geometries.
 * </ul>
 * An example where the default approximation is not close is:
 * <pre>
 *   A = LINESTRING (0 0, 100 0, 10 100, 10 100)
 *   B = LINESTRING (0 100, 0 10, 80 10)
 *   
 *   DHD(A, B) = 22.360679774997898
 *   HD(A, B) ~= 47.8
 * </pre>
 */
public class DiscreteHausdorffDistance
{
  public static double distance(Geometry g0, Geometry g1)
  {
    DiscreteHausdorffDistance dist = new DiscreteHausdorffDistance(g0, g1);
    return dist.distance();
  }

  public static double distance(Geometry g0, Geometry g1, double densifyFrac)
  {
    DiscreteHausdorffDistance dist = new DiscreteHausdorffDistance(g0, g1);
    dist.setDensifyFraction(densifyFrac);
    return dist.distance();
  }

  private Geometry g0;
  private Geometry g1;
  private PointPairDistance ptDist = new PointPairDistance();
  
  /**
   * Value of 0.0 indicates that no densification should take place
   */
  private double densifyFrac = 0.0;

  public DiscreteHausdorffDistance(Geometry g0, Geometry g1)
  {
    this.g0 = g0;
    this.g1 = g1;
  }

  /**
   * Sets the fraction by which to densify each segment.
   * Each segment will be (virtually) split into a number of equal-length
   * subsegments, whose fraction of the total length is closest
   * to the given fraction.
   * 
   * @param densifyPercent
   */
  public void setDensifyFraction(double densifyFrac)
  {
    if (densifyFrac > 1.0 
        || densifyFrac <= 0.0)
      throw new IllegalArgumentException("Fraction is not in range (0.0 - 1.0]");
        
    this.densifyFrac = densifyFrac;
  }
  
  public double distance() 
  { 
    compute(g0, g1);
    return ptDist.getDistance(); 
  }

  public double orientedDistance() 
  { 
    computeOrientedDistance(g0, g1, ptDist);
    return ptDist.getDistance(); 
  }

  public Coordinate[] getCoordinates() { return ptDist.getCoordinates(); }

  private void compute(Geometry g0, Geometry g1)
  {
    computeOrientedDistance(g0, g1, ptDist);
    computeOrientedDistance(g1, g0, ptDist);
  }

  private void computeOrientedDistance(Geometry discreteGeom, Geometry geom, PointPairDistance ptDist)
  {
    MaxPointDistanceFilter distFilter = new MaxPointDistanceFilter(geom);
    discreteGeom.apply(distFilter);
    ptDist.setMaximum(distFilter.getMaxPointDistance());
    
    if (densifyFrac > 0) {
      MaxDensifiedByFractionDistanceFilter fracFilter = new MaxDensifiedByFractionDistanceFilter(geom, densifyFrac);
      discreteGeom.apply(fracFilter);
      ptDist.setMaximum(fracFilter.getMaxPointDistance());
      
    }
  }

  public static class MaxPointDistanceFilter
      implements CoordinateFilter
  {
    private PointPairDistance maxPtDist = new PointPairDistance();
    private PointPairDistance minPtDist = new PointPairDistance();
    private DistanceToPoint euclideanDist = new DistanceToPoint();
    private Geometry geom;

    public MaxPointDistanceFilter(Geometry geom)
    {
      this.geom = geom;
    }

    public void filter(Coordinate pt)
    {
      minPtDist.initialize();
      DistanceToPoint.computeDistance(geom, pt, minPtDist);
      maxPtDist.setMaximum(minPtDist);
    }

    public PointPairDistance getMaxPointDistance() { return maxPtDist; }
  }
  
  public static class MaxDensifiedByFractionDistanceFilter 
  implements CoordinateSequenceFilter 
  {
  private PointPairDistance maxPtDist = new PointPairDistance();
  private PointPairDistance minPtDist = new PointPairDistance();
  private Geometry geom;
  private int numSubSegs = 0;

  public MaxDensifiedByFractionDistanceFilter(Geometry geom, double fraction) {
    this.geom = geom;
    numSubSegs = (int) Math.rint(1.0/fraction);
  }

  public void filter(CoordinateSequence seq, int index) 
  {
    /**
     * This logic also handles skipping Point geometries
     */
    if (index == 0)
      return;
    
    Coordinate p0 = seq.getCoordinate(index - 1);
    Coordinate p1 = seq.getCoordinate(index);
    
    double delx = (p1.x - p0.x)/numSubSegs;
    double dely = (p1.y - p0.y)/numSubSegs;

    for (int i = 0; i < numSubSegs; i++) {
      double x = p0.x + i*delx;
      double y = p0.y + i*dely;
      Coordinate pt = new Coordinate(x, y);
      minPtDist.initialize();
      DistanceToPoint.computeDistance(geom, pt, minPtDist);
      maxPtDist.setMaximum(minPtDist);  
    }
    
    
  }

  public boolean isGeometryChanged() { return false; }
  
  public boolean isDone() { return false; }
  
  public PointPairDistance getMaxPointDistance() {
    return maxPtDist;
  }
}

}
