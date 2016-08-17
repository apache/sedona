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
package com.vividsolutions.jts.awt;

import java.awt.Rectangle;
import java.awt.Shape;
import java.awt.geom.AffineTransform;
import java.awt.geom.GeneralPath;
import java.awt.geom.PathIterator;
import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.util.Collection;
import java.util.Iterator;

import com.vividsolutions.jts.geom.Coordinate;

/**
 * A {@link Shape} which represents a polygon which may contain holes.
 * Provided because the standard AWT Polygon class does not support holes.
 * 
 * @author Martin Davis
 *
 */
public class PolygonShape implements Shape 
{
  // use a GeneralPath with a winding rule, since it supports floating point coordinates
    private GeneralPath polygonPath;
    private GeneralPath ringPath;
    
    /**
     * Creates a new polygon {@link Shape}.
     * 
     * @param shellVertices the vertices of the shell 
     * @param holeVerticesCollection a collection of Coordinate[] for each hole
     */
    public PolygonShape(Coordinate[] shellVertices,
        Collection holeVerticesCollection) 
    {
        polygonPath = toPath(shellVertices);

        for (Iterator i = holeVerticesCollection.iterator(); i.hasNext();) {
            Coordinate[] holeVertices = (Coordinate[]) i.next();
            polygonPath.append(toPath(holeVertices), false);
        }
    }

    public PolygonShape() 
    {
    }

    void addToRing(Point2D p)
    {
    	if (ringPath == null) {
    		ringPath = new GeneralPath(GeneralPath.WIND_EVEN_ODD);
    		ringPath.moveTo((float) p.getX(), (float) p.getY());
    	}
    	else {
    		ringPath.lineTo((float) p.getX(), (float) p.getY());
    	}
    }
    
    void endRing()
    {
      ringPath.closePath();
    	if (polygonPath == null) {
    		polygonPath = ringPath;
    	}
    	else {
    		polygonPath.append(ringPath, false);
    	}
    	ringPath = null;
    }
    
    /**
     * Creates a GeneralPath representing a polygon ring 
     * having the given coordinate sequence.
     * Uses the GeneralPath.WIND_EVEN_ODD winding rule.
     * 
     * @param coordinates a coordinate sequence
     * @return the path for the coordinate sequence
     */
    private GeneralPath toPath(Coordinate[] coordinates) {
      GeneralPath path = new GeneralPath(GeneralPath.WIND_EVEN_ODD, coordinates.length);

      if (coordinates.length > 0) {
        path.moveTo((float) coordinates[0].x, (float) coordinates[0].y);
        for (int i = 0; i < coordinates.length; i++) {
          path.lineTo((float) coordinates[i].x, (float) coordinates[i].y);
        }
      }
      return path;
  }

    public Rectangle getBounds() {
      return polygonPath.getBounds();
    }

    public Rectangle2D getBounds2D() {
        return polygonPath.getBounds2D();
    }

    public boolean contains(double x, double y) {
      return polygonPath.contains(x, y);
    }

    public boolean contains(Point2D p) {
      return polygonPath.contains(p);
    }

    public boolean intersects(double x, double y, double w, double h) {
      return polygonPath.intersects(x, y, w, h);
    }

    public boolean intersects(Rectangle2D r) {
      return polygonPath.intersects(r);
    }

    public boolean contains(double x, double y, double w, double h) {
      return polygonPath.contains(x, y, w, h);
    }

    public boolean contains(Rectangle2D r) {
      return polygonPath.contains(r);
    }

    public PathIterator getPathIterator(AffineTransform at) {
        return polygonPath.getPathIterator(at);
    }

    public PathIterator getPathIterator(AffineTransform at, double flatness) {
    	return getPathIterator(at, flatness);
    }
}
