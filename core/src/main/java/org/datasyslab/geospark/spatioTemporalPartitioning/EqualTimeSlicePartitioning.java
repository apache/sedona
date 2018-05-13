/*
 * FILE: EqualIntervalPartitioning
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

package org.datasyslab.geospark.spatioTemporalPartitioning;

import java.util.ArrayList;
import java.util.List;

import org.datasyslab.geospark.SpatioTemporalObjects.Cube;


/**
 * The Class EqualIntervalPartitioning.
 */
public class EqualTimeSlicePartitioning {

    /**
     * The grids.
     */
    final List<Cube> grids = new ArrayList<>();

    /**
     * Instantiates a new equal time interval partitioning.
     *
     * @param samples the sample
     * @param boundary the boundary
     * @param partitions the partitions
     */
    public EqualTimeSlicePartitioning(List<Cube> samples, Cube boundary, final int partitions)
    {
        double minTime = boundary.getMinZ();
        double maxTime = boundary.getMaxZ();

        double interval = (maxTime - minTime) / partitions;
        ArrayList<ArrayList<Cube>> parts = new ArrayList<ArrayList<Cube>>();

        for (int i = 0; i < partitions; i++) {
            parts.add(new ArrayList<Cube>());
            Cube cube = new Cube(Double.MIN_VALUE, Double.MAX_VALUE,
                    Double.MIN_VALUE, Double.MAX_VALUE,
                    minTime + i * interval, minTime + (i + 1) * interval);
            grids.add(cube);
        }
    }

    /**
     * Gets the Sample List bounds.
     *
     * @return the bounds
     */
    private Cube getBoundery(List<Cube> cubes) {
        double minx = Double.MAX_VALUE;
        double maxx = Double.MIN_VALUE;
        double miny = Double.MAX_VALUE;
        double maxy = Double.MIN_VALUE;
        double minz = Double.MAX_VALUE;
        double maxz = Double.MIN_VALUE;
        if (cubes == null || cubes.size() == 0) {
            return new Cube(0, 0, 0, 0, 0, 0);
        }
        for (int i = 0; i < cubes.size(); i++) {
            minx = minx < cubes.get(i).getMinX() ? minx : cubes.get(i).getMinX();
            maxx = maxx > cubes.get(i).getMaxX() ? maxx : cubes.get(i).getMaxX();
            miny = miny < cubes.get(i).getMinY() ? miny : cubes.get(i).getMinY();
            maxy = maxy > cubes.get(i).getMaxY() ? maxy : cubes.get(i).getMaxY();
            minz = minz < cubes.get(i).getMinZ() ? minz : cubes.get(i).getMinZ();
            maxz = maxz > cubes.get(i).getMaxZ() ? maxz : cubes.get(i).getMaxZ();
        }
        return new Cube(minx, maxx, miny, maxy, minz, maxz);
    }

    /**
     * Gets the grids.
     *
     * @return the grids
     */
    public List<Cube> getGrids()
    {
        return this.grids;
    }

}
