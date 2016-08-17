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

import com.vividsolutions.jts.algorithm.Angle;
import com.vividsolutions.jts.algorithm.CGAlgorithms;
import com.vividsolutions.jts.algorithm.HCoordinate;
import com.vividsolutions.jts.algorithm.LineIntersector;
import com.vividsolutions.jts.algorithm.NotRepresentableException;
import com.vividsolutions.jts.algorithm.RobustLineIntersector;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LineSegment;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.geomgraph.Position;
import com.vividsolutions.jts.util.Debug;

/**
 * Generates segments which form an offset curve.
 * Supports all end cap and join options 
 * provided for buffering.
 * This algorithm implements various heuristics to 
 * produce smoother, simpler curves which are
 * still within a reasonable tolerance of the 
 * true curve.
 * 
 * @author Martin Davis
 *
 */
class OffsetSegmentGenerator 
{

  /**
   * Factor which controls how close offset segments can be to
   * skip adding a filler or mitre.
   */
  private static final double OFFSET_SEGMENT_SEPARATION_FACTOR = 1.0E-3;
  
  /**
   * Factor which controls how close curve vertices on inside turns can be to be snapped 
   */
  private static final double INSIDE_TURN_VERTEX_SNAP_DISTANCE_FACTOR = 1.0E-3;

  /**
   * Factor which controls how close curve vertices can be to be snapped
   */
  private static final double CURVE_VERTEX_SNAP_DISTANCE_FACTOR = 1.0E-6;

  /**
   * Factor which determines how short closing segs can be for round buffers
   */
  private static final int MAX_CLOSING_SEG_LEN_FACTOR = 80;

  /**
   * the max error of approximation (distance) between a quad segment and the true fillet curve
   */
  private double maxCurveSegmentError = 0.0;

  /**
   * The angle quantum with which to approximate a fillet curve
   * (based on the input # of quadrant segments)
   */
  private double filletAngleQuantum;

  /**
   * The Closing Segment Length Factor controls how long
   * "closing segments" are.  Closing segments are added
   * at the middle of inside corners to ensure a smoother
   * boundary for the buffer offset curve. 
   * In some cases (particularly for round joins with default-or-better
   * quantization) the closing segments can be made quite short.
   * This substantially improves performance (due to fewer intersections being created).
   * 
   * A closingSegFactor of 0 results in lines to the corner vertex
   * A closingSegFactor of 1 results in lines halfway to the corner vertex
   * A closingSegFactor of 80 results in lines 1/81 of the way to the corner vertex
   * (this option is reasonable for the very common default situation of round joins
   * and quadrantSegs >= 8)
   */
  private int closingSegLengthFactor = 1;

  private OffsetSegmentString segList;
  private double distance = 0.0;
  private PrecisionModel precisionModel;
  private BufferParameters bufParams;
  private LineIntersector li;

  private Coordinate s0, s1, s2;
  private LineSegment seg0 = new LineSegment();
  private LineSegment seg1 = new LineSegment();
  private LineSegment offset0 = new LineSegment();
  private LineSegment offset1 = new LineSegment();
  private int side = 0;
  private boolean hasNarrowConcaveAngle = false;

  public OffsetSegmentGenerator(PrecisionModel precisionModel,
      BufferParameters bufParams, double distance) {
    this.precisionModel = precisionModel;
    this.bufParams = bufParams;

    // compute intersections in full precision, to provide accuracy
    // the points are rounded as they are inserted into the curve line
    li = new RobustLineIntersector();
    filletAngleQuantum = Math.PI / 2.0 / bufParams.getQuadrantSegments();

    /**
     * Non-round joins cause issues with short closing segments, so don't use
     * them. In any case, non-round joins only really make sense for relatively
     * small buffer distances.
     */
    if (bufParams.getQuadrantSegments() >= 8
        && bufParams.getJoinStyle() == BufferParameters.JOIN_ROUND)
      closingSegLengthFactor = MAX_CLOSING_SEG_LEN_FACTOR;
    init(distance);
  }

  /**
   * Tests whether the input has a narrow concave angle
   * (relative to the offset distance).
   * In this case the generated offset curve will contain self-intersections
   * and heuristic closing segments.
   * This is expected behaviour in the case of Buffer curves. 
   * For pure Offset Curves,
   * the output needs to be further treated 
   * before it can be used. 
   * 
   * @return true if the input has a narrow concave angle
   */
  public boolean hasNarrowConcaveAngle()
  {
    return hasNarrowConcaveAngle;
  }
  
  private void init(double distance)
  {
    this.distance = distance;
    maxCurveSegmentError = distance * (1 - Math.cos(filletAngleQuantum / 2.0));
    segList = new OffsetSegmentString();
    segList.setPrecisionModel(precisionModel);
    /**
     * Choose the min vertex separation as a small fraction of the offset distance.
     */
    segList.setMinimumVertexDistance(distance * CURVE_VERTEX_SNAP_DISTANCE_FACTOR);
  }


  public void initSideSegments(Coordinate s1, Coordinate s2, int side)
  {
    this.s1 = s1;
    this.s2 = s2;
    this.side = side;
    seg1.setCoordinates(s1, s2);
    computeOffsetSegment(seg1, side, distance, offset1);
  }

  public Coordinate[] getCoordinates()
  {
    Coordinate[] pts = segList.getCoordinates();
    return pts;
  }
  
  public void closeRing()
  {
    segList.closeRing();
  }
  
  public void addSegments(Coordinate[] pt, boolean isForward)
  {
    segList.addPts(pt, isForward);
  }
  
  public void addFirstSegment()
  {
    segList.addPt(offset1.p0);
  }
  
  /**
   * Add last offset point
   */
  public void addLastSegment()
  {
    segList.addPt(offset1.p1);
  }

  //private static double MAX_CLOSING_SEG_LEN = 3.0;

  public void addNextSegment(Coordinate p, boolean addStartPoint)
  {
    // s0-s1-s2 are the coordinates of the previous segment and the current one
    s0 = s1;
    s1 = s2;
    s2 = p;
    seg0.setCoordinates(s0, s1);
    computeOffsetSegment(seg0, side, distance, offset0);
    seg1.setCoordinates(s1, s2);
    computeOffsetSegment(seg1, side, distance, offset1);

    // do nothing if points are equal
    if (s1.equals(s2)) return;

    int orientation = CGAlgorithms.computeOrientation(s0, s1, s2);
    boolean outsideTurn =
          (orientation == CGAlgorithms.CLOCKWISE        && side == Position.LEFT)
      ||  (orientation == CGAlgorithms.COUNTERCLOCKWISE && side == Position.RIGHT);

    if (orientation == 0) { // lines are collinear
      addCollinear(addStartPoint);
    }
    else if (outsideTurn) 
    {
      addOutsideTurn(orientation, addStartPoint);
    }
    else { // inside turn
      addInsideTurn(orientation, addStartPoint);
    }
  }
  
  private void addCollinear(boolean addStartPoint)
  {
    /**
     * This test could probably be done more efficiently,
     * but the situation of exact collinearity should be fairly rare.
     */
    li.computeIntersection(s0, s1, s1, s2);
    int numInt = li.getIntersectionNum();
    /**
     * if numInt is < 2, the lines are parallel and in the same direction. In
     * this case the point can be ignored, since the offset lines will also be
     * parallel.
     */
    if (numInt >= 2) {
      /**
       * segments are collinear but reversing. 
       * Add an "end-cap" fillet
       * all the way around to other direction This case should ONLY happen
       * for LineStrings, so the orientation is always CW. (Polygons can never
       * have two consecutive segments which are parallel but reversed,
       * because that would be a self intersection.
       * 
       */
      if (bufParams.getJoinStyle() == BufferParameters.JOIN_BEVEL 
          || bufParams.getJoinStyle() == BufferParameters.JOIN_MITRE) {
        if (addStartPoint) segList.addPt(offset0.p1);
        segList.addPt(offset1.p0);
      }
      else {
        addFillet(s1, offset0.p1, offset1.p0, CGAlgorithms.CLOCKWISE, distance);
      }
    }
  }
  
  /**
   * Adds the offset points for an outside (convex) turn
   * 
   * @param orientation
   * @param addStartPoint
   */
  private void addOutsideTurn(int orientation, boolean addStartPoint)
  {
    /**
     * Heuristic: If offset endpoints are very close together, 
     * just use one of them as the corner vertex.
     * This avoids problems with computing mitre corners in the case
     * where the two segments are almost parallel 
     * (which is hard to compute a robust intersection for).
     */
    if (offset0.p1.distance(offset1.p0) < distance * OFFSET_SEGMENT_SEPARATION_FACTOR) {
      segList.addPt(offset0.p1);
      return;
    }
    
    if (bufParams.getJoinStyle() == BufferParameters.JOIN_MITRE) {
      addMitreJoin(s1, offset0, offset1, distance);
    }
    else if (bufParams.getJoinStyle() == BufferParameters.JOIN_BEVEL){
      addBevelJoin(offset0, offset1);
    }
    else {
    // add a circular fillet connecting the endpoints of the offset segments
     if (addStartPoint) segList.addPt(offset0.p1);
      // TESTING - comment out to produce beveled joins
      addFillet(s1, offset0.p1, offset1.p0, orientation, distance);
      segList.addPt(offset1.p0);
    }
  }
  
  /**
   * Adds the offset points for an inside (concave) turn.
   * 
   * @param orientation
   * @param addStartPoint
   */
  private void addInsideTurn(int orientation, boolean addStartPoint) {
    /**
     * add intersection point of offset segments (if any)
     */
    li.computeIntersection(offset0.p0, offset0.p1, offset1.p0, offset1.p1);
    if (li.hasIntersection()) {
      segList.addPt(li.getIntersection(0));
    }
    else {
      /**
       * If no intersection is detected, 
       * it means the angle is so small and/or the offset so
       * large that the offsets segments don't intersect. 
       * In this case we must
       * add a "closing segment" to make sure the buffer curve is continuous,
       * fairly smooth (e.g. no sharp reversals in direction)
       * and tracks the buffer correctly around the corner. The curve connects
       * the endpoints of the segment offsets to points
       * which lie toward the centre point of the corner.
       * The joining curve will not appear in the final buffer outline, since it
       * is completely internal to the buffer polygon.
       * 
       * In complex buffer cases the closing segment may cut across many other
       * segments in the generated offset curve.  In order to improve the 
       * performance of the noding, the closing segment should be kept as short as possible.
       * (But not too short, since that would defeat its purpose).
       * This is the purpose of the closingSegFactor heuristic value.
       */ 
      
       /** 
       * The intersection test above is vulnerable to robustness errors; i.e. it
       * may be that the offsets should intersect very close to their endpoints,
       * but aren't reported as such due to rounding. To handle this situation
       * appropriately, we use the following test: If the offset points are very
       * close, don't add closing segments but simply use one of the offset
       * points
       */
      hasNarrowConcaveAngle = true;
      //System.out.println("NARROW ANGLE - distance = " + distance);
      if (offset0.p1.distance(offset1.p0) < distance
          * INSIDE_TURN_VERTEX_SNAP_DISTANCE_FACTOR) {
        segList.addPt(offset0.p1);
      } else {
        // add endpoint of this segment offset
        segList.addPt(offset0.p1);
        
        /**
         * Add "closing segment" of required length.
         */
        if (closingSegLengthFactor > 0) {
          Coordinate mid0 = new Coordinate((closingSegLengthFactor * offset0.p1.x + s1.x)/(closingSegLengthFactor + 1), 
              (closingSegLengthFactor*offset0.p1.y + s1.y)/(closingSegLengthFactor + 1));
          segList.addPt(mid0);
          Coordinate mid1 = new Coordinate((closingSegLengthFactor*offset1.p0.x + s1.x)/(closingSegLengthFactor + 1), 
             (closingSegLengthFactor*offset1.p0.y + s1.y)/(closingSegLengthFactor + 1));
          segList.addPt(mid1);
        }
        else {
          /**
           * This branch is not expected to be used except for testing purposes.
           * It is equivalent to the JTS 1.9 logic for closing segments
           * (which results in very poor performance for large buffer distances)
           */
          segList.addPt(s1);
        }
        
        //*/  
        // add start point of next segment offset
        segList.addPt(offset1.p0);
      }
    }
  }
  

  /**
   * Compute an offset segment for an input segment on a given side and at a given distance.
   * The offset points are computed in full double precision, for accuracy.
   *
   * @param seg the segment to offset
   * @param side the side of the segment ({@link Position}) the offset lies on
   * @param distance the offset distance
   * @param offset the points computed for the offset segment
   */
  private void computeOffsetSegment(LineSegment seg, int side, double distance, LineSegment offset)
  {
    int sideSign = side == Position.LEFT ? 1 : -1;
    double dx = seg.p1.x - seg.p0.x;
    double dy = seg.p1.y - seg.p0.y;
    double len = Math.sqrt(dx * dx + dy * dy);
    // u is the vector that is the length of the offset, in the direction of the segment
    double ux = sideSign * distance * dx / len;
    double uy = sideSign * distance * dy / len;
    offset.p0.x = seg.p0.x - uy;
    offset.p0.y = seg.p0.y + ux;
    offset.p1.x = seg.p1.x - uy;
    offset.p1.y = seg.p1.y + ux;
  }

  /**
   * Add an end cap around point p1, terminating a line segment coming from p0
   */
  public void addLineEndCap(Coordinate p0, Coordinate p1)
  {
    LineSegment seg = new LineSegment(p0, p1);

    LineSegment offsetL = new LineSegment();
    computeOffsetSegment(seg, Position.LEFT, distance, offsetL);
    LineSegment offsetR = new LineSegment();
    computeOffsetSegment(seg, Position.RIGHT, distance, offsetR);

    double dx = p1.x - p0.x;
    double dy = p1.y - p0.y;
    double angle = Math.atan2(dy, dx);

    switch (bufParams.getEndCapStyle()) {
      case BufferParameters.CAP_ROUND:
        // add offset seg points with a fillet between them
        segList.addPt(offsetL.p1);
        addFillet(p1, angle + Math.PI / 2, angle - Math.PI / 2, CGAlgorithms.CLOCKWISE, distance);
        segList.addPt(offsetR.p1);
        break;
      case BufferParameters.CAP_FLAT:
        // only offset segment points are added
        segList.addPt(offsetL.p1);
        segList.addPt(offsetR.p1);
        break;
      case BufferParameters.CAP_SQUARE:
        // add a square defined by extensions of the offset segment endpoints
        Coordinate squareCapSideOffset = new Coordinate();
        squareCapSideOffset.x = Math.abs(distance) * Math.cos(angle);
        squareCapSideOffset.y = Math.abs(distance) * Math.sin(angle);

        Coordinate squareCapLOffset = new Coordinate(
            offsetL.p1.x + squareCapSideOffset.x,
            offsetL.p1.y + squareCapSideOffset.y);
        Coordinate squareCapROffset = new Coordinate(
            offsetR.p1.x + squareCapSideOffset.x,
            offsetR.p1.y + squareCapSideOffset.y);
        segList.addPt(squareCapLOffset);
        segList.addPt(squareCapROffset);
        break;

    }
  }
  /**
   * Adds a mitre join connecting the two reflex offset segments.
   * The mitre will be beveled if it exceeds the mitre ratio limit.
   * 
   * @param offset0 the first offset segment
   * @param offset1 the second offset segment
   * @param distance the offset distance
   */
  private void addMitreJoin(Coordinate p, 
      LineSegment offset0, 
      LineSegment offset1,
      double distance)
  {
    boolean isMitreWithinLimit = true;
    Coordinate intPt = null;
  
    /**
     * This computation is unstable if the offset segments are nearly collinear.
     * Howver, this situation should have been eliminated earlier by the check for 
     * whether the offset segment endpoints are almost coincident
     */
    try {
     intPt = HCoordinate.intersection(offset0.p0, 
        offset0.p1, offset1.p0, offset1.p1);
     
     double mitreRatio = distance <= 0.0 ? 1.0
         : intPt.distance(p) / Math.abs(distance);
     
     if (mitreRatio > bufParams.getMitreLimit())
       isMitreWithinLimit = false;
    }
    catch (NotRepresentableException ex) {
      intPt = new Coordinate(0,0);
      isMitreWithinLimit = false;
    }
    
    if (isMitreWithinLimit) {
      segList.addPt(intPt);
    }
    else {
      addLimitedMitreJoin(offset0, offset1, distance, bufParams.getMitreLimit());
//      addBevelJoin(offset0, offset1);
    }
  }
  
  
  /**
   * Adds a limited mitre join connecting the two reflex offset segments.
   * A limited mitre is a mitre which is beveled at the distance
   * determined by the mitre ratio limit.
   * 
   * @param offset0 the first offset segment
   * @param offset1 the second offset segment
   * @param distance the offset distance
   * @param mitreLimit the mitre limit ratio
   */
  private void addLimitedMitreJoin( 
      LineSegment offset0, 
      LineSegment offset1,
      double distance,
      double mitreLimit)
  {
    Coordinate basePt = seg0.p1;
    
    double ang0 = Angle.angle(basePt, seg0.p0);
    double ang1 = Angle.angle(basePt, seg1.p1);
    
    // oriented angle between segments
    double angDiff = Angle.angleBetweenOriented(seg0.p0, basePt, seg1.p1);
    // half of the interior angle
    double angDiffHalf = angDiff / 2;
  
    // angle for bisector of the interior angle between the segments
    double midAng = Angle.normalize(ang0 + angDiffHalf);
    // rotating this by PI gives the bisector of the reflex angle
    double mitreMidAng = Angle.normalize(midAng + Math.PI);
    
    // the miterLimit determines the distance to the mitre bevel
    double mitreDist = mitreLimit * distance;
    // the bevel delta is the difference between the buffer distance
    // and half of the length of the bevel segment
    double bevelDelta = mitreDist * Math.abs(Math.sin(angDiffHalf));
    double bevelHalfLen = distance - bevelDelta;

    // compute the midpoint of the bevel segment
    double bevelMidX = basePt.x + mitreDist * Math.cos(mitreMidAng);
    double bevelMidY = basePt.y + mitreDist * Math.sin(mitreMidAng);
    Coordinate bevelMidPt = new Coordinate(bevelMidX, bevelMidY);
    
    // compute the mitre midline segment from the corner point to the bevel segment midpoint
    LineSegment mitreMidLine = new LineSegment(basePt, bevelMidPt);
    
    // finally the bevel segment endpoints are computed as offsets from 
    // the mitre midline
    Coordinate bevelEndLeft = mitreMidLine.pointAlongOffset(1.0, bevelHalfLen);
    Coordinate bevelEndRight = mitreMidLine.pointAlongOffset(1.0, -bevelHalfLen);
    
    if (side == Position.LEFT) {
      segList.addPt(bevelEndLeft);
      segList.addPt(bevelEndRight);
    }
    else {
      segList.addPt(bevelEndRight);
      segList.addPt(bevelEndLeft);     
    }
  }
  
  /**
   * Adds a bevel join connecting the two offset segments
   * around a reflex corner.
   * 
   * @param offset0 the first offset segment
   * @param offset1 the second offset segment
   */
  private void addBevelJoin( 
      LineSegment offset0, 
      LineSegment offset1)
  {
     segList.addPt(offset0.p1);
     segList.addPt(offset1.p0);        
  }
  
  
  /**
   * Add points for a circular fillet around a reflex corner.
   * Adds the start and end points
   * 
   * @param p base point of curve
   * @param p0 start point of fillet curve
   * @param p1 endpoint of fillet curve
   * @param direction the orientation of the fillet
   * @param radius the radius of the fillet
   */
  private void addFillet(Coordinate p, Coordinate p0, Coordinate p1, int direction, double radius)
  {
    double dx0 = p0.x - p.x;
    double dy0 = p0.y - p.y;
    double startAngle = Math.atan2(dy0, dx0);
    double dx1 = p1.x - p.x;
    double dy1 = p1.y - p.y;
    double endAngle = Math.atan2(dy1, dx1);

    if (direction == CGAlgorithms.CLOCKWISE) {
      if (startAngle <= endAngle) startAngle += 2.0 * Math.PI;
    }
    else {    // direction == COUNTERCLOCKWISE
      if (startAngle >= endAngle) startAngle -= 2.0 * Math.PI;
    }
    segList.addPt(p0);
    addFillet(p, startAngle, endAngle, direction, radius);
    segList.addPt(p1);
  }

  /**
   * Adds points for a circular fillet arc
   * between two specified angles.  
   * The start and end point for the fillet are not added -
   * the caller must add them if required.
   *
   * @param direction is -1 for a CW angle, 1 for a CCW angle
   * @param radius the radius of the fillet
   */
  private void addFillet(Coordinate p, double startAngle, double endAngle, int direction, double radius)
  {
    int directionFactor = direction == CGAlgorithms.CLOCKWISE ? -1 : 1;

    double totalAngle = Math.abs(startAngle - endAngle);
    int nSegs = (int) (totalAngle / filletAngleQuantum + 0.5);

    if (nSegs < 1) return;    // no segments because angle is less than increment - nothing to do!

    double initAngle, currAngleInc;

    // choose angle increment so that each segment has equal length
    initAngle = 0.0;
    currAngleInc = totalAngle / nSegs;

    double currAngle = initAngle;
    Coordinate pt = new Coordinate();
    while (currAngle < totalAngle) {
      double angle = startAngle + directionFactor * currAngle;
      pt.x = p.x + radius * Math.cos(angle);
      pt.y = p.y + radius * Math.sin(angle);
      segList.addPt(pt);
      currAngle += currAngleInc;
    }
  }


  /**
   * Creates a CW circle around a point
   */
  public void createCircle(Coordinate p)
  {
    // add start point
    Coordinate pt = new Coordinate(p.x + distance, p.y);
    segList.addPt(pt);
    addFillet(p, 0.0, 2.0 * Math.PI, -1, distance);
    segList.closeRing();
  }

  /**
   * Creates a CW square around a point
   */
  public void createSquare(Coordinate p)
  {
    segList.addPt(new Coordinate(p.x + distance, p.y + distance));
    segList.addPt(new Coordinate(p.x + distance, p.y - distance));
    segList.addPt(new Coordinate(p.x - distance, p.y - distance));
    segList.addPt(new Coordinate(p.x - distance, p.y + distance));
    segList.closeRing();
  }
}
