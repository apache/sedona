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
package com.vividsolutions.jts.precision;

import com.vividsolutions.jts.algorithm.CGAlgorithms;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineSegment;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Lineal;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.index.strtree.ItemBoundable;
import com.vividsolutions.jts.index.strtree.ItemDistance;
import com.vividsolutions.jts.index.strtree.STRtree;
import com.vividsolutions.jts.operation.distance.FacetSequence;
import com.vividsolutions.jts.operation.distance.FacetSequenceTreeBuilder;

/**
 * Computes the Minimum Clearance of a {@link Geometry}.
 * <p>
 * The <b>Minimum Clearance</b> is a measure of
 * what magnitude of perturbation of
 * the vertices of a geometry can be tolerated
 * before the geometry becomes topologically invalid.
 * The smaller the Minimum Clearance distance, 
 * the less vertex pertubation the geometry can tolerate
 * before becoming invalid.
 * <p>
 * The concept was introduced by Thompson and Van Oosterom
 * [TV06], based on earlier work by Milenkovic [Mi88].
 * <p>
 * The Minimum Clearance of a geometry G 
 * is defined to be the value <i>r</i>
 * such that "the movement of all points by a distance
 * of <i>r</i> in any direction will 
 * guarantee to leave the geometry valid" [TV06].
 * An equivalent constructive definition [Mi88] is that
 * <i>r</i> is the largest value such:
 * <ol>
 * <li>No two distinct vertices of G are closer than <i>r</i>
 * <li>No vertex of G is closer than <i>r</i> to an edge of G
 * of which the vertex is not an endpoint
 * </ol>
 * The following image shows an example of the Minimum Clearance
 * of a simple polygon.
 * <p>
 * <center><img src='doc-files/minClearance.png'></center>
 * <p>
 * If G has only a single vertex (i.e. is a
 * {@link Point}), the value of the minimum clearance 
 * is {@link Double#MAX_VALUE}.
 * <p>
 * If G is a {@link Puntal} or {@link Lineal} geometry, 
 * then in fact no amount of perturbation
 * will render the geometry invalid.  
 * In this case a Minimum Clearance is still computed
 * based on the vertex and segment distances
 * according to the constructive definition.
 * <p>
 * It is possible for no Minimum Clearance to exist.
 * For instance, a {@link MultiPoint} with all members identical
 * has no Minimum Clearance
 * (i.e. no amount of perturbation will cause 
 * the member points to become non-identical).
 * Empty geometries also have no such distance.
 * The lack of a meaningful MinimumClearance distance is detected
 * and suitable values are returned by 
 * {@link #getDistance()} and {@link #getLine()}.
 * <p>
 * The computation of Minimum Clearance utilizes 
 * the {@link STRtree#nearestNeighbour(ItemDistance)}
 * method to provide good performance even for
 * large inputs.
 * <p>
 * An interesting note is that for the case of {@link MultiPoint}s, 
 * the computed Minimum Clearance line
 * effectively determines the Nearest Neighbours in the collection. 
 *
 * <h3>References</h3>
 * <ul>
 * <li>[Mi88] Milenkovic, V. J., 
 * <i>Verifiable implementations of geometric algorithms 
 * using finite precision arithmetic</i>.
 * in Artificial Intelligence, 377-401. 1988
 * <li>[TV06] Thompson, Rod and van Oosterom, Peter,
 * <i>Interchange of Spatial Data-Inhibiting Factors</i>,
 * Agile 2006, Visegrad, Hungary. 2006
 * </ul>
 * 
 * @author Martin Davis
 *
 */
public class MinimumClearance 
{
  /**
   * Computes the Minimum Clearance distance for 
   * the given Geometry.
   * 
   * @param g the input geometry
   * @return the Minimum Clearance distance
   */
  public static double getDistance(Geometry g)
  {
    MinimumClearance rp = new MinimumClearance(g);
    return rp.getDistance();
  }
  
  /**
   * Gets a LineString containing two points
   * which are at the Minimum Clearance distance
   * for the given Geometry.
   * 
   * @param g the input geometry
   * @return the value of the minimum clearance distance
   * or <tt>LINESTRING EMPTY</tt> if no Minimum Clearance distance exists
   */
  public static Geometry getLine(Geometry g)
  {
    MinimumClearance rp = new MinimumClearance(g);
    return rp.getLine();
  }
  
  private Geometry inputGeom;
  private double minClearance;
  private Coordinate[] minClearancePts;
  
  /**
   * Creates an object to compute the Minimum Clearance
   * for the given Geometry
   * 
   * @param geom the input geometry
   */
  public MinimumClearance(Geometry geom)
  {
    inputGeom = geom;
  }
  
  /**
   * Gets the Minimum Clearance distance.
   * <p>
   * If no distance exists 
   * (e.g. in the case of two identical points)
   * <tt>Double.MAX_VALUE</tt> is returned.
   * 
   * @return the value of the minimum clearance distance
   * or <tt>Double.MAX_VALUE</tt> if no Minimum Clearance distance exists
   */
  public double getDistance()
  {
    compute();
    return minClearance;
  }
  
  /**
   * Gets a LineString containing two points
   * which are at the Minimum Clearance distance.
   * <p>
   * If no distance could be found 
   * (e.g. in the case of two identical points)
   * <tt>LINESTRING EMPTY</tt> is returned.
   * 
   * @return the value of the minimum clearance distance
   * or <tt>LINESTRING EMPTY</tt> if no Minimum Clearance distance exists
   */
  public LineString getLine()
  {
    compute();
    // return empty line string if no min pts where found
    if (minClearancePts == null || minClearancePts[0] == null)
      return inputGeom.getFactory().createLineString((Coordinate[]) null);
    return inputGeom.getFactory().createLineString(minClearancePts);
  }
  
  private void compute()
  {
    // already computed
    if (minClearancePts != null) return;
    
    // initialize to "No Distance Exists" state
    minClearancePts = new Coordinate[2];
    minClearance = Double.MAX_VALUE;
    
    // handle empty geometries
    if (inputGeom.isEmpty()) {
      return;
    }
    
    STRtree geomTree = FacetSequenceTreeBuilder.build(inputGeom);
    
    Object[] nearest = geomTree.nearestNeighbour(new MinClearanceDistance());
    MinClearanceDistance mcd = new MinClearanceDistance();
    minClearance = mcd.distance(
        (FacetSequence) nearest[0],
        (FacetSequence) nearest[1]);
    minClearancePts = mcd.getCoordinates();
  }
  
  /**
   * Implements the MinimumClearance distance function:
   * <ul>
   * <li>dist(p1, p2) = 
   * <ul>
   * <li>p1 != p2 : p1.distance(p2)
   * <li>p1 == p2 : Double.MAX
   * </ul>
   * <li>dist(p, seg) =
   * <ul>
   * <li>p != seq.p1 && p != seg.p2 : seg.distance(p)
   * <li>ELSE : Double.MAX
   * </ul>
   * </ul>
   * Also computes the values of the nearest points, if any.
   * 
   * @author Martin Davis
   *
   */
  private static class MinClearanceDistance
  implements ItemDistance
  {
    private double minDist = Double.MAX_VALUE;
    private Coordinate[] minPts = new Coordinate[2];
    
    public Coordinate[] getCoordinates()
    {
      return minPts;
    }
    
    public double distance(ItemBoundable b1, ItemBoundable b2) {
      FacetSequence fs1 = (FacetSequence) b1.getItem();
      FacetSequence fs2 = (FacetSequence) b2.getItem();
      minDist = Double.MAX_VALUE;
      return distance(fs1, fs2);
    }
    
    public double distance(FacetSequence fs1, FacetSequence fs2) {
      
      // compute MinClearance distance metric

      vertexDistance(fs1, fs2);
      if (fs1.size() == 1 && fs2.size() == 1) return minDist;
      if (minDist <= 0.0) return minDist;
      segmentDistance(fs1, fs2);
      if (minDist <= 0.0) return minDist;
      segmentDistance(fs2, fs1);
      return minDist;
    }
    
    private double vertexDistance(FacetSequence fs1, FacetSequence fs2) {
      for (int i1 = 0; i1 < fs1.size(); i1++) {
        for (int i2 = 0; i2 < fs2.size(); i2++) {
          Coordinate p1 = fs1.getCoordinate(i1);
          Coordinate p2 = fs2.getCoordinate(i2);
          if (! p1.equals2D(p2)) {
            double d = p1.distance(p2);
            if (d < minDist) {
              minDist = d;
              minPts[0] = p1;
              minPts[1] = p2;
              if (d == 0.0)
                return d;
            }
          }
        }
      }
      return minDist;
     }
      
     private double segmentDistance(FacetSequence fs1, FacetSequence fs2) {
        for (int i1 = 0; i1 < fs1.size(); i1++) {
          for (int i2 = 1; i2 < fs2.size(); i2++) {
            
            Coordinate p = fs1.getCoordinate(i1);
            
            Coordinate seg0 = fs2.getCoordinate(i2-1);
            Coordinate seg1 = fs2.getCoordinate(i2);
            
            if (! (p.equals2D(seg0) || p.equals2D(seg1))) {
              double d = CGAlgorithms.distancePointLine(p, seg0, seg1);
              if (d < minDist) {
                minDist = d;
                updatePts(p, seg0, seg1);
                if (d == 0.0)
                  return d;
              }
            }
          }
        }
        return minDist;
       }
     
     private void updatePts(Coordinate p, Coordinate seg0, Coordinate seg1)
     {
       minPts[0] = p;
       LineSegment seg = new LineSegment(seg0, seg1);
       minPts[1] = new Coordinate(seg.closestPoint(p));       
     }

       
     }
  
    
  }
  

