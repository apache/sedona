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

package com.vividsolutions.jts.operation.overlay.snap;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.operation.overlay.OverlayOp;
import com.vividsolutions.jts.operation.overlay.validate.OverlayResultValidator;


/**
 * Performs an overlay operation using snapping and enhanced precision
 * to improve the robustness of the result.
 * This class only uses snapping
 * if an error is detected when running the standard JTS overlay code.
 * Errors detected include thrown exceptions 
 * (in particular, {@link TopologyException})
 * and invalid overlay computations.
 *     
 * @author Martin Davis
 * @version 1.7
 */
public class SnapIfNeededOverlayOp
{
  public static Geometry overlayOp(Geometry g0, Geometry g1, int opCode)
  {
  	SnapIfNeededOverlayOp op = new SnapIfNeededOverlayOp(g0, g1);
  	return op.getResultGeometry(opCode);
  }

  public static Geometry intersection(Geometry g0, Geometry g1)
  {
     return overlayOp(g0, g1, OverlayOp.INTERSECTION);
  }

  public static Geometry union(Geometry g0, Geometry g1)
  {
     return overlayOp(g0, g1, OverlayOp.UNION);
  }

  public static Geometry difference(Geometry g0, Geometry g1)
  {
     return overlayOp(g0, g1, OverlayOp.DIFFERENCE);
  }

  public static Geometry symDifference(Geometry g0, Geometry g1)
  {
     return overlayOp(g0, g1, OverlayOp.SYMDIFFERENCE);
  }
  
  private Geometry[] geom = new Geometry[2];

  public SnapIfNeededOverlayOp(Geometry g1, Geometry g2)
  {
    geom[0] = g1;
    geom[1] = g2;
  }

  public Geometry getResultGeometry(int opCode)
  {
    Geometry result = null;
    boolean isSuccess = false;
    RuntimeException savedException = null;
    try {
      // try basic operation with input geometries
      result = OverlayOp.overlayOp(geom[0], geom[1], opCode); 
      boolean isValid = true;
      // not needed if noding validation is used
//      boolean isValid = OverlayResultValidator.isValid(geom[0], geom[1], OverlayOp.INTERSECTION, result);
      if (isValid)
      	isSuccess = true;
    }
    catch (RuntimeException ex) {
    	savedException = ex;
    	// ignore this exception, since the operation will be rerun
//    	System.out.println(ex.getMessage());
//    	ex.printStackTrace();
    	//System.out.println(ex.getMessage());
    	//System.out.println("Geom 0: " + geom[0]);
    	//System.out.println("Geom 1: " + geom[1]);
    }
    if (! isSuccess) {
    	// this may still throw an exception
    	// if so, throw the original exception since it has the input coordinates
    	try {
    		result = SnapOverlayOp.overlayOp(geom[0], geom[1], opCode);
    	}
    	catch (RuntimeException ex) {
    		throw savedException;
    	}
    }
    return result;
  }
}
