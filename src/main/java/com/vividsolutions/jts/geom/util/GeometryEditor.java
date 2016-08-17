
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
package com.vividsolutions.jts.geom.util;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.util.Assert;

import java.util.ArrayList;


/**
 * A class which supports creating new {@link Geometry}s 
 * which are modifications of existing ones,
 * maintaining the same type structure.
 * Geometry objects are intended to be treated as immutable.
 * This class "modifies" Geometrys
 * by traversing them, applying a user-defined
 * {@link GeometryEditorOperation}, {@link CoordinateSequenceOperation} or {@link CoordinateOperation}  
 * and creating new Geometrys with the same structure but
 * (possibly) modified components.
 * <p>
 * Examples of the kinds of modifications which can be made are:
 * <ul>
 * <li>the values of the coordinates may be changed.
 *     The editor does not check whether changing coordinate values makes the result Geometry invalid
 * <li>the coordinate lists may be changed
 *     (e.g. by adding, deleting or modifying coordinates).
 *     The modifed coordinate lists must be consistent with their original parent component
 *     (e.g. a <tt>LinearRing</tt> must always have at least 4 coordinates, and the first and last
 *     coordinate must be equal)
 * <li>components of the original geometry may be deleted
 *    (e.g. holes may be removed from a Polygon, or LineStrings removed from a MultiLineString).
 *     Deletions will be propagated up the component tree appropriately.
 * </ul>
 * All changes must be consistent with the original Geometry's structure
 * (e.g. a <tt>Polygon</tt> cannot be collapsed into a <tt>LineString</tt>).
 * If changing the structure is required, use a {@link GeometryTransformer}.
 * <p>
 * This class supports creating an edited Geometry
 * using a different <code>GeometryFactory</code> via the {@link #GeometryEditor(GeometryFactory)}
 * constructor.  
 * Examples of situations where this is required is if the geometry is 
 * transformed to a new SRID and/or a new PrecisionModel.
 * <p>
 * <b>Usage Notes</b>
 * <ul>
 * <li>The resulting Geometry is not checked for validity.
 * If validity needs to be enforced, the new Geometry's 
 * {@link Geometry#isValid} method should be called.
 * <li>By default the UserData of the input geometry is not copied to the result.
 * </ul>
 * 
 * @see GeometryTransformer
 * @see Geometry#isValid
 *
 * @version 1.7
 */
public class GeometryEditor
{
  /**
   * The factory used to create the modified Geometry.
   * If <tt>null</tt> the GeometryFactory of the input is used.
   */
  private GeometryFactory factory = null;
  private boolean isUserDataCopied = false;

  /**
   * Creates a new GeometryEditor object which will create
   * edited {@link Geometry}s with the same {@link GeometryFactory} as the input Geometry.
   */
  public GeometryEditor()
  {
  }

  /**
   * Creates a new GeometryEditor object which will create
   * edited {@link Geometry}s with the given {@link GeometryFactory}.
   *
   * @param factory the GeometryFactory to create  edited Geometrys with
   */
  public GeometryEditor(GeometryFactory factory)
  {
    this.factory = factory;
  }

  /**
   * Sets whether the User Data is copied to the edit result.
   * Only the object reference is copied.
   * 
   * @param isUserDataCopied true if the input user data should be copied.
   */
  public void setCopyUserData(boolean isUserDataCopied)
  {
    this.isUserDataCopied = isUserDataCopied;
  }
  
  /**
   * Edit the input {@link Geometry} with the given edit operation.
   * Clients can create subclasses of {@link GeometryEditorOperation} or
   * {@link CoordinateOperation} to perform required modifications.
   *
   * @param geometry the Geometry to edit
   * @param operation the edit operation to carry out
   * @return a new {@link Geometry} which is the result of the editing (which may be empty)
   */
  public Geometry edit(Geometry geometry, GeometryEditorOperation operation)
  {
    // nothing to do
    if (geometry == null) return null;
    
    Geometry result = editInternal(geometry, operation);
    if (isUserDataCopied) {
      result.setUserData(geometry.getUserData());
    }
    return result;
  }
  
  private Geometry editInternal(Geometry geometry, GeometryEditorOperation operation)
  {
    // if client did not supply a GeometryFactory, use the one from the input Geometry
    if (factory == null)
      factory = geometry.getFactory();

    if (geometry instanceof GeometryCollection) {
      return editGeometryCollection((GeometryCollection) geometry,
                                    operation);
    }

    if (geometry instanceof Polygon) {
      return editPolygon((Polygon) geometry, operation);
    }

    if (geometry instanceof Point) {
      return operation.edit(geometry, factory);
    }

    if (geometry instanceof LineString) {
      return operation.edit(geometry, factory);
    }

    Assert.shouldNeverReachHere("Unsupported Geometry class: " + geometry.getClass().getName());
    return null;
  }

  private Polygon editPolygon(Polygon polygon,
                              GeometryEditorOperation operation) {
    Polygon newPolygon = (Polygon) operation.edit(polygon, factory);
    // create one if needed
    if (newPolygon == null)
      newPolygon = factory.createPolygon((CoordinateSequence) null);
    if (newPolygon.isEmpty()) {
      //RemoveSelectedPlugIn relies on this behaviour. [Jon Aquino]
      return newPolygon;
    }

    LinearRing shell = (LinearRing) edit(newPolygon.getExteriorRing(), operation);
    if (shell == null || shell.isEmpty()) {
      //RemoveSelectedPlugIn relies on this behaviour. [Jon Aquino]
      return factory.createPolygon(null, null);
    }

    ArrayList holes = new ArrayList();
    for (int i = 0; i < newPolygon.getNumInteriorRing(); i++) {
      LinearRing hole = (LinearRing) edit(newPolygon.getInteriorRingN(i), operation);
      if (hole == null || hole.isEmpty()) {
        continue;
      }
      holes.add(hole);
    }

    return factory.createPolygon(shell,
                                 (LinearRing[]) holes.toArray(new LinearRing[] {  }));
  }

  private GeometryCollection editGeometryCollection(
      GeometryCollection collection, GeometryEditorOperation operation) {
    // first edit the entire collection
    // MD - not sure why this is done - could just check original collection?
    GeometryCollection collectionForType = (GeometryCollection) operation.edit(collection,
        factory);
    
    // edit the component geometries
    ArrayList geometries = new ArrayList();
    for (int i = 0; i < collectionForType.getNumGeometries(); i++) {
      Geometry geometry = edit(collectionForType.getGeometryN(i), operation);
      if (geometry == null || geometry.isEmpty()) {
        continue;
      }
      geometries.add(geometry);
    }

    if (collectionForType.getClass() == MultiPoint.class) {
      return factory.createMultiPoint((Point[]) geometries.toArray(
            new Point[] {  }));
    }
    if (collectionForType.getClass() == MultiLineString.class) {
      return factory.createMultiLineString((LineString[]) geometries.toArray(
            new LineString[] {  }));
    }
    if (collectionForType.getClass() == MultiPolygon.class) {
      return factory.createMultiPolygon((Polygon[]) geometries.toArray(
            new Polygon[] {  }));
    }
    return factory.createGeometryCollection((Geometry[]) geometries.toArray(
          new Geometry[] {  }));
  }

  /**
   * A interface which specifies an edit operation for Geometries.
   *
   * @version 1.7
   */
  public interface GeometryEditorOperation
  {
    /**
     * Edits a Geometry by returning a new Geometry with a modification.
     * The returned geometry may be:
     * <ul>
     * <li>the input geometry itself
     * The returned Geometry might be the same as the Geometry passed in.
     * It may be <code>null</code> if the geometry is to be deleted.
     *
     * @param geometry the Geometry to modify
     * @param factory the factory with which to construct the modified Geometry
     * (may be different to the factory of the input geometry)
     * @return a new Geometry which is a modification of the input Geometry
     * @return null if the Geometry is to be deleted completely
     */
    Geometry edit(Geometry geometry, GeometryFactory factory);
  }

  /**
   * A GeometryEditorOperation which does not modify
   * the input geometry.
   * This can be used for simple changes of 
   * GeometryFactory (including PrecisionModel and SRID).
   * 
   * @author mbdavis
   *
   */
  public static class NoOpGeometryOperation
  implements GeometryEditorOperation
  {
  	public Geometry edit(Geometry geometry, GeometryFactory factory)
  	{
  		return geometry;
  	}
  }
  
  /**
   * A {@link GeometryEditorOperation} which edits the coordinate list of a {@link Geometry}.
   * Operates on Geometry subclasses which contains a single coordinate list.
   */
  public abstract static class CoordinateOperation
      implements GeometryEditorOperation
  {
    public final Geometry edit(Geometry geometry, GeometryFactory factory) {
      if (geometry instanceof LinearRing) {
        return factory.createLinearRing(edit(geometry.getCoordinates(),
            geometry));
      }

      if (geometry instanceof LineString) {
        return factory.createLineString(edit(geometry.getCoordinates(),
            geometry));
      }

      if (geometry instanceof Point) {
        Coordinate[] newCoordinates = edit(geometry.getCoordinates(),
            geometry);

        return factory.createPoint((newCoordinates.length > 0)
                                   ? newCoordinates[0] : null);
      }

      return geometry;
    }

    /**
     * Edits the array of {@link Coordinate}s from a {@link Geometry}.
     * <p>
     * If it is desired to preserve the immutability of Geometrys,
     * if the coordinates are changed a new array should be created
     * and returned.
     *
     * @param coordinates the coordinate array to operate on
     * @param geometry the geometry containing the coordinate list
     * @return an edited coordinate array (which may be the same as the input)
     */
    public abstract Coordinate[] edit(Coordinate[] coordinates,
                                      Geometry geometry);
  }
  
  /**
   * A {@link GeometryEditorOperation} which edits the {@link CoordinateSequence}
   * of a {@link Geometry}.
   * Operates on Geometry subclasses which contains a single coordinate list.
   */
  public abstract static class CoordinateSequenceOperation
      implements GeometryEditorOperation
  {
    public final Geometry edit(Geometry geometry, GeometryFactory factory) {
      if (geometry instanceof LinearRing) {
        return factory.createLinearRing(edit(
            ((LinearRing)geometry).getCoordinateSequence(),
            geometry));
      }

      if (geometry instanceof LineString) {
        return factory.createLineString(edit(
            ((LineString)geometry).getCoordinateSequence(),
            geometry));
      }

      if (geometry instanceof Point) {
        return factory.createPoint(edit(
            ((Point)geometry).getCoordinateSequence(),
            geometry));
      }

      return geometry;
    }

    /**
     * Edits a {@link CoordinateSequence} from a {@link Geometry}.
     *
     * @param coordseq the coordinate array to operate on
     * @param geometry the geometry containing the coordinate list
     * @return an edited coordinate sequence (which may be the same as the input)
     */
    public abstract CoordinateSequence edit(CoordinateSequence coordSeq,
                                      Geometry geometry);
  }
}
