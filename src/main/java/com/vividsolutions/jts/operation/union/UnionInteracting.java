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

package com.vividsolutions.jts.operation.union;

import java.util.ArrayList;
import java.util.List;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.geom.util.GeometryCombiner;

/**
 * Experimental code to union MultiPolygons
 * with processing limited to the elements which actually interact.
 * 
 * Not currently used, since it doesn't seem to offer much of a performance advantage.
 * 
 * @author mbdavis
 *
 */
public class UnionInteracting 
{
	public static Geometry union(Geometry g0, Geometry g1)
	{
		UnionInteracting uue = new UnionInteracting(g0, g1);
		return uue.union();
	}

	
	private GeometryFactory geomFactory;
	
	private Geometry g0;
	private Geometry g1;
	
	private boolean[] interacts0;
	private boolean[] interacts1;
	
	public UnionInteracting(Geometry g0, Geometry g1)
	{
		this.g0 = g0;
		this.g1 = g1;
		geomFactory = g0.getFactory();
		interacts0 = new boolean[g0.getNumGeometries()];
		interacts1 = new boolean[g1.getNumGeometries()];
	}
	
	public Geometry union()
	{
		computeInteracting();
		
		// check for all interacting or none interacting!
		
		Geometry int0 = extractElements(g0, interacts0, true);
		Geometry int1 = extractElements(g1, interacts1, true);
		
//		System.out.println(int0);
//		System.out.println(int1);

		if (int0.isEmpty() || int1.isEmpty()) {
			System.out.println("found empty!");
//			computeInteracting();
		}
//		if (! int0.isValid()) {
			//System.out.println(int0);
			//throw new RuntimeException("invalid geom!");
//		}
		
		Geometry union = int0.union(int1);
		//Geometry union = bufferUnion(int0, int1);
		
		Geometry disjoint0 = extractElements(g0, interacts0, false);
		Geometry disjoint1 = extractElements(g1, interacts1, false);
		
  	Geometry overallUnion = GeometryCombiner.combine(union, disjoint0, disjoint1);
  	
  	return overallUnion;

	}
	
  private Geometry bufferUnion(Geometry g0, Geometry g1)
  {
  	GeometryFactory factory = g0.getFactory();
  	Geometry gColl = factory.createGeometryCollection(new Geometry[] { g0, g1 } );
  	Geometry unionAll = gColl.buffer(0.0);
    return unionAll;
  }

	private void computeInteracting()
	{
		for (int i = 0; i < g0.getNumGeometries(); i++) {
			Geometry elem = g0.getGeometryN(i);
			interacts0[i] = computeInteracting(elem);
		}
	}
	
	private boolean computeInteracting(Geometry elem0)
	{
		boolean interactsWithAny = false;
		for (int i = 0; i < g1.getNumGeometries(); i++) {
			Geometry elem1 = g1.getGeometryN(i);
			boolean interacts = elem1.getEnvelopeInternal().intersects(elem0.getEnvelopeInternal());
			if (interacts) interacts1[i] = true;
			if (interacts) 
				interactsWithAny = true;
		}
		return interactsWithAny;
	}
	
  private Geometry extractElements(Geometry geom, 
  		boolean[] interacts, boolean isInteracting)
  {
  	List extractedGeoms = new ArrayList();
  	for (int i = 0; i < geom.getNumGeometries(); i++) { 
  		Geometry elem = geom.getGeometryN(i);
  		if (interacts[i] == isInteracting)
  			extractedGeoms.add(elem);
  	}
  	return geomFactory.buildGeometry(extractedGeoms);
  }

	
}
