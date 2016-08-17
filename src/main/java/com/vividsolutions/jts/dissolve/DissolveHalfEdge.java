package com.vividsolutions.jts.dissolve;

import com.vividsolutions.jts.edgegraph.MarkHalfEdge;
import com.vividsolutions.jts.geom.Coordinate;


/**
 * A HalfEdge which carries information
 * required to support {@link LineDissolver}.
 * 
 * @author Martin Davis
 *
 */
class DissolveHalfEdge extends MarkHalfEdge
{
  private boolean isStart = false;
  
  public DissolveHalfEdge(Coordinate orig) {
    super(orig);
  }

  /**
   * Tests whether this edge is the starting segment
   * in a LineString being dissolved.
   * 
   * @return true if this edge is a start segment
   */
  public boolean isStart()
  {
    return isStart;
  }
  
  /**
   * Sets this edge to be the start segment of an input LineString.
   */
  public void setStart()
  {
    isStart = true;
  }
}
