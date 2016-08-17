
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

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.geom.util.*;

/**
 * Reduces the precision of a {@link Geometry}
 * according to the supplied {@link PrecisionModel},
 * ensuring that the result is topologically valid.
 *
 * @version 1.12
 */
public class GeometryPrecisionReducer
{
	/**
	 * Convenience method for doing precision reduction 
   * on a single geometry,
	 * with collapses removed 
   * and keeping the geometry precision model the same,
   * and preserving polygonal topology.
	 * 
	 * @param g the geometry to reduce
	 * @param precModel the precision model to use
	 * @return the reduced geometry
	 */
	public static Geometry reduce(Geometry g, PrecisionModel precModel)
	{
		GeometryPrecisionReducer reducer = new GeometryPrecisionReducer(precModel);
		return reducer.reduce(g);
	}
	
	/**
	 * Convenience method for doing pointwise precision reduction 
   * on a single geometry,
	 * with collapses removed 
   * and keeping the geometry precision model the same,
   * but NOT preserving valid polygonal topology.
	 * 
	 * @param g the geometry to reduce
	 * @param precModel the precision model to use
	 * @return the reduced geometry
	 */
	public static Geometry reducePointwise(Geometry g, PrecisionModel precModel)
	{
		GeometryPrecisionReducer reducer = new GeometryPrecisionReducer(precModel);
		reducer.setPointwise(true);
		return reducer.reduce(g);
	}
	
  private PrecisionModel targetPM;
  private boolean removeCollapsed = true;
  private boolean changePrecisionModel = false;
  private boolean isPointwise = false;

  public GeometryPrecisionReducer(PrecisionModel pm)
  {
    targetPM = pm;
  }

  /**
   * Sets whether the reduction will result in collapsed components
   * being removed completely, or simply being collapsed to an (invalid)
   * Geometry of the same type.
   * The default is to remove collapsed components.
   *
   * @param removeCollapsed if <code>true</code> collapsed components will be removed
   */
  public void setRemoveCollapsedComponents(boolean removeCollapsed)
  {
    this.removeCollapsed = removeCollapsed;
  }

  /**
   * Sets whether the {@link PrecisionModel} of the new reduced Geometry
   * will be changed to be the {@link PrecisionModel} supplied to
   * specify the precision reduction.
   * <p>  
   * The default is to <b>not</b> change the precision model
   *
   * @param changePrecisionModel if <code>true</code> the precision model of the created Geometry will be the
   * the precisionModel supplied in the constructor.
   */
  public void setChangePrecisionModel(boolean changePrecisionModel)
  {
    this.changePrecisionModel = changePrecisionModel;
  }

  /**
   * Sets whether the precision reduction will be done 
   * in pointwise fashion only.  
   * Pointwise precision reduction reduces the precision
   * of the individual coordinates only, but does
   * not attempt to recreate valid topology.
   * This is only relevant for geometries containing polygonal components.
   * 
   * @param isPointwise if reduction should be done pointwise only
   */
  public void setPointwise(boolean isPointwise)
  {
    this.isPointwise = isPointwise;
  }

  public Geometry reduce(Geometry geom)
  {
    Geometry reducePW = reducePointwise(geom);
    if (isPointwise)
    	return reducePW;
    
    //TODO: handle GeometryCollections containing polys
    if (! (reducePW instanceof Polygonal))
    	return reducePW;
    
    // Geometry is polygonal - test if topology needs to be fixed
    if (reducePW.isValid()) return reducePW;
    
    // hack to fix topology.  
    // TODO: implement snap-rounding and use that.
    return fixPolygonalTopology(reducePW);
  }

  private Geometry reducePointwise(Geometry geom)
  {
    GeometryEditor geomEdit;
    if (changePrecisionModel) {
    	GeometryFactory newFactory = createFactory(geom.getFactory(), targetPM);
      geomEdit = new GeometryEditor(newFactory);
    }
    else
      // don't change geometry factory
      geomEdit = new GeometryEditor();

    /**
     * For polygonal geometries, collapses are always removed, in order
     * to produce correct topology
     */
    boolean finalRemoveCollapsed = removeCollapsed;
    if (geom.getDimension() >= 2)
    	finalRemoveCollapsed = true;
    
    Geometry reduceGeom = geomEdit.edit(geom, 
    		new PrecisionReducerCoordinateOperation(targetPM, finalRemoveCollapsed));
    
    return reduceGeom;
  }
  
  private Geometry fixPolygonalTopology(Geometry geom)
  {
  	/**
  	 * If precision model was *not* changed, need to flip
  	 * geometry to targetPM, buffer in that model, then flip back
  	 */
  	Geometry geomToBuffer = geom;
  	if (! changePrecisionModel) {
  		geomToBuffer = changePM(geom, targetPM);
  	}
  	
  	Geometry bufGeom = geomToBuffer.buffer(0);
  	
  	Geometry finalGeom = bufGeom;
  	if (! changePrecisionModel) {
  	  // a slick way to copy the geometry with the original precision factory
  		finalGeom = geom.getFactory().createGeometry(bufGeom);
  	}
  	return finalGeom;
  }
  
  /**
   * Duplicates a geometry to one that uses a different PrecisionModel,
   * without changing any coordinate values.
   * 
   * @param geom the geometry to duplicate
   * @param newPM the precision model to use
   * @return the geometry value with a new precision model
   */
  private Geometry changePM(Geometry geom, PrecisionModel newPM)
  {
  	GeometryEditor geomEditor = createEditor(geom.getFactory(), newPM);
  	// this operation changes the PM for the entire geometry tree
  	return geomEditor.edit(geom, new GeometryEditor.NoOpGeometryOperation());
  }
  
  private GeometryEditor createEditor(GeometryFactory geomFactory, PrecisionModel newPM)
  {
    // no need to change if precision model is the same
  	if (geomFactory.getPrecisionModel() == newPM)
  		return new GeometryEditor();
  	// otherwise create a geometry editor which changes PrecisionModel
  	GeometryFactory newFactory = createFactory(geomFactory, newPM);
  	GeometryEditor geomEdit = new GeometryEditor(newFactory);
    return geomEdit;
  }
  
  private GeometryFactory createFactory(GeometryFactory inputFactory, PrecisionModel pm)
  {
    GeometryFactory newFactory 
  	= new GeometryFactory(pm, 
  			inputFactory.getSRID(),
  			inputFactory.getCoordinateSequenceFactory());
    return newFactory;
  }
  
}
