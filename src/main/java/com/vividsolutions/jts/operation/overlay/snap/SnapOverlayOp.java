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
import com.vividsolutions.jts.precision.CommonBitsRemover;

/**
 * Performs an overlay operation using snapping and enhanced precision
 * to improve the robustness of the result.
 * This class <i>always</i> uses snapping.  
 * This is less performant than the standard JTS overlay code, 
 * and may even introduce errors which were not present in the original data.
 * For this reason, this class should only be used 
 * if the standard overlay code fails to produce a correct result. 
 *  
 * @author Martin Davis
 * @version 1.7
 */
public class SnapOverlayOp
{
  public static Geometry overlayOp(Geometry g0, Geometry g1, int opCode)
  {
  	SnapOverlayOp op = new SnapOverlayOp(g0, g1);
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
  private double snapTolerance;

  public SnapOverlayOp(Geometry g1, Geometry g2)
  {
    geom[0] = g1;
    geom[1] = g2;
    computeSnapTolerance();
  }
  private void computeSnapTolerance() 
  {
		snapTolerance = GeometrySnapper.computeOverlaySnapTolerance(geom[0], geom[1]);

		// System.out.println("Snap tol = " + snapTolerance);
	}

  public Geometry getResultGeometry(int opCode)
  {
//  	Geometry[] selfSnapGeom = new Geometry[] { selfSnap(geom[0]), selfSnap(geom[1])};
    Geometry[] prepGeom = snap(geom);
    Geometry result = OverlayOp.overlayOp(prepGeom[0], prepGeom[1], opCode);
    return prepareResult(result);	
  }
  
  private Geometry selfSnap(Geometry geom)
  {
    GeometrySnapper snapper0 = new GeometrySnapper(geom);
    Geometry snapGeom = snapper0.snapTo(geom, snapTolerance);
    //System.out.println("Self-snapped: " + snapGeom);
    //System.out.println();
    return snapGeom;
  }
  
  private Geometry[] snap(Geometry[] geom)
  {
    Geometry[] remGeom = removeCommonBits(geom);
  	
  	// MD - testing only
//  	Geometry[] remGeom = geom;
    
    Geometry[] snapGeom = GeometrySnapper.snap(remGeom[0], remGeom[1], snapTolerance);
    // MD - may want to do this at some point, but it adds cycles
//    checkValid(snapGeom[0]);
//    checkValid(snapGeom[1]);

    /*
    System.out.println("Snapped geoms: ");
    System.out.println(snapGeom[0]);
    System.out.println(snapGeom[1]);
    */
    return snapGeom;
  }

  private Geometry prepareResult(Geometry geom)
  {
    cbr.addCommonBits(geom);
    return geom;
  }

  private CommonBitsRemover cbr;

  private Geometry[] removeCommonBits(Geometry[] geom)
  {
    cbr = new CommonBitsRemover();
    cbr.add(geom[0]);
    cbr.add(geom[1]);
    Geometry remGeom[] = new Geometry[2];
    remGeom[0] = cbr.removeCommonBits((Geometry) geom[0].clone());
    remGeom[1] = cbr.removeCommonBits((Geometry) geom[1].clone());
    return remGeom;
  }
  
  private void checkValid(Geometry g)
  {
  	if (! g.isValid()) {
  		System.out.println("Snapped geometry is invalid");
  	}
  }
}
