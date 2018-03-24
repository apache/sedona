/*
 * FILE: EqualPartitioning
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

import com.vividsolutions.jts.geom.Envelope;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

// TODO: Auto-generated Javadoc

/**
 * The Class EqualPartitioning.
 */
public class EqualPartitioning
        implements Serializable
{

    /**
     * The grids.
     */
    List<Envelope> grids = new ArrayList<Envelope>();

    /**
     * Instantiates a new equal partitioning.
     *
     * @param boundary the boundary
     * @param partitions the partitions
     */
    public EqualPartitioning(Envelope boundary, int partitions)
    {
        //Local variable should be declared here
        Double root = Math.sqrt(partitions);
        int partitionsAxis;
        double intervalX;
        double intervalY;

        //Calculate how many bounds should be on each axis
        partitionsAxis = root.intValue();
        intervalX = (boundary.getMaxX() - boundary.getMinX()) / partitionsAxis;
        intervalY = (boundary.getMaxY() - boundary.getMinY()) / partitionsAxis;
        //System.out.println("Boundary: "+boundary+"root: "+root+" interval: "+intervalX+","+intervalY);
        for (int i = 0; i < partitionsAxis; i++) {
            for (int j = 0; j < partitionsAxis; j++) {
                Envelope grid = new Envelope(boundary.getMinX() + intervalX * i, boundary.getMinX() + intervalX * (i + 1), boundary.getMinY() + intervalY * j, boundary.getMinY() + intervalY * (j + 1));
                //System.out.println("Grid: "+grid);
                grids.add(grid);
            }
            //System.out.println("Finish one column/one certain x");
        }
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
