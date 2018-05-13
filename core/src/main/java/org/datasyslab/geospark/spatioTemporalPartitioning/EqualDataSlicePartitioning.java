/*
 * FILE: EqualDataSlicePartitioning
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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.datasyslab.geospark.SpatioTemporalObjects.Cube;

/**
 * The Class EqualDataSlicePartitioning.
 */
public class EqualDataSlicePartitioning {

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
    public EqualDataSlicePartitioning(List<Cube> samples, Cube boundary, final int partitions)
    {
        double minTime = boundary.getMinZ();
        double maxTime = boundary.getMaxZ();

        List<Cube> sortSample = new ArrayList<Cube>(samples);

        sortByZ(sortSample);

        int step = sortSample.size() / partitions;

        for (int i = 0; i < partitions; i++) {
            int indexLow = i * step;
            int indexHigh = (i + 1) * step;
            if (indexHigh >= sortSample.size()) {
                indexHigh = sortSample.size() - 1;
            }
            Cube cube = new Cube(Double.MIN_VALUE, Double.MAX_VALUE,
                    Double.MIN_VALUE, Double.MAX_VALUE,
                    sortSample.get(indexLow).getMinZ(), sortSample.get(indexHigh).getMaxZ());
            grids.add(cube);
        }

    }

    /**
     * Gets the Sort Sample List.
     *
     * @return the bounds
     */
    private void sortByZ(List<Cube> sample) {
        Collections.sort(sample, new Comparator<Cube>() {
            @Override
            public int compare(Cube cube1, Cube cube2)
            {
                int res = 0;
                if (cube1.getMinZ() < cube2.getMinZ()) {
                    res = -1;
                } else if (cube1.getMinZ() == cube2.getMinZ()) {
                    res = 0;
                } else if (cube1.getMinZ() > cube2.getMinZ()) {
                    res = 1;
                }
                return  res;
            }
        });
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
