/*
 * FILE: VoronoiPartitioning
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package org.datasyslab.geospark.spatialPartitioning;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.triangulate.VoronoiDiagramBuilder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

// TODO: Auto-generated Javadoc

/**
 * The Class VoronoiPartitioning.
 */
public class VoronoiPartitioning
        implements Serializable
{

    /**
     * The grids.
     */
    List<Envelope> grids = new ArrayList<Envelope>();

    /**
     * Instantiates a new voronoi partitioning.
     *
     * @param samples the sample list
     * @param partitions the partitions
     * @throws Exception the exception
     */
    public VoronoiPartitioning(List<Envelope> samples, int partitions)
            throws Exception
    {
        GeometryFactory fact = new GeometryFactory();
        ArrayList<Point> subSampleList = new ArrayList<Point>();
        MultiPoint mp;

        //Take a subsample accoring to the partitions
        for (int i = 0; i < samples.size(); i = i + samples.size() / partitions) {
            Envelope envelope = samples.get(i);
            Coordinate coordinate = new Coordinate((envelope.getMinX() + envelope.getMaxX()) / 2.0, (envelope.getMinY() + envelope.getMaxY()) / 2.0);
            subSampleList.add(fact.createPoint(coordinate));
        }

        mp = fact.createMultiPoint(subSampleList.toArray(new Point[subSampleList.size()]));
        VoronoiDiagramBuilder voronoiBuilder = new VoronoiDiagramBuilder();
        voronoiBuilder.setSites(mp);
        Geometry voronoiDiagram = voronoiBuilder.getDiagram(fact);
        for (int i = 0; i < voronoiDiagram.getNumGeometries(); i++) {
            Polygon poly = (Polygon) voronoiDiagram.getGeometryN(i);
            grids.add(poly.getEnvelopeInternal());
        }
        //grids.add(new EnvelopeWithGrid(boundary,grids.size()));
    }

    /**
     * Gets the grids.
     *
     * @return the grids
     */
    public List<Envelope> getGrids()
    {

        return this.grids;
    }
}
