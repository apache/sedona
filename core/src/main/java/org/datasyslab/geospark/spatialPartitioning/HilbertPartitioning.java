/*
 * FILE: HilbertPartitioning
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
import java.util.Arrays;
import java.util.List;

public class HilbertPartitioning
        implements Serializable
{

    private static final int GRID_RESOLUTION = Short.MAX_VALUE;

    /**
     * The splits.
     */
    //Partition ID
    protected int[] splits;

    /**
     * The grids.
     */
    List<Envelope> grids = new ArrayList<>();

    /**
     * Instantiates a new hilbert partitioning.
     *
     * @param samples the sample list
     * @param boundary the boundary
     * @param partitions the partitions
     * @throws Exception the exception
     */
    public HilbertPartitioning(List<Envelope> samples, Envelope boundary, int partitions)
            throws Exception
    {
        //this.boundary=boundary;
        int[] hValues = new int[samples.size()];
        for (int i = 0; i < samples.size(); i++) {
            hValues[i] = computeHValue(boundary, samples.get(i));
        }

        createFromHValues(hValues, partitions);

        // Aggregate samples by partition; compute bounding box of all the samples in each partition
        Envelope[] gridWithoutID = new Envelope[partitions];
        for (Envelope sample : samples) {
            int partitionID = gridID(boundary, sample, splits);
            Envelope current = gridWithoutID[partitionID];
            if (current == null) {
                gridWithoutID[partitionID] = sample;
            }
            else {
                gridWithoutID[partitionID] = updateEnvelope(current, sample);
            }
        }

        for (Envelope envelope : gridWithoutID) {
            this.grids.add(envelope);
        }
    }

    /**
     * Creates the from H values.
     *
     * @param hValues the h values
     * @param partitions the partitions
     */
    protected void createFromHValues(int[] hValues, int partitions)
    {
        Arrays.sort(hValues);

        this.splits = new int[partitions];
        int maxH = 0x7fffffff;
        for (int i = 0; i < splits.length; i++) {
            int quantile = (int) ((long) (i + 1) * hValues.length / partitions);
            this.splits[i] = quantile == hValues.length ? maxH : hValues[quantile];
        }
    }

    /**
     * Compute H value.
     *
     * @param n the n
     * @param x the x
     * @param y the y
     * @return the int
     */
    public static int computeHValue(int n, int x, int y)
    {
        int h = 0;
        for (int s = n / 2; s > 0; s /= 2) {
            int rx = (x & s) > 0 ? 1 : 0;
            int ry = (y & s) > 0 ? 1 : 0;
            h += s * s * ((3 * rx) ^ ry);

            // Rotate
            if (ry == 0) {
                if (rx == 1) {
                    x = n - 1 - x;
                    y = n - 1 - y;
                }

                //Swap x and y
                int t = x;
                x = y;
                y = t;
            }
        }
        return h;
    }

    /**
     * Gets the partition bounds.
     *
     * @return the partition bounds
     */
    public int[] getPartitionBounds()
    {
        return splits;
    }

    /**
     * Location mapping.
     *
     * @param axisMin the axis min
     * @param axisLocation the axis location
     * @param axisMax the axis max
     * @return the int
     */
    public static int locationMapping(double axisMin, double axisLocation, double axisMax)
    {
        Double gridLocation = (axisLocation - axisMin) * GRID_RESOLUTION / (axisMax - axisMin);
        return gridLocation.intValue();
    }

    /**
     * Grid ID.
     *
     * @param boundary the boundary
     * @param spatialObject the spatial object
     * @param partitionBounds the partition bounds
     * @return the int
     * @throws Exception the exception
     */
    public static int gridID(Envelope boundary, Envelope spatialObject, int[] partitionBounds)
            throws Exception
    {
        int hValue = computeHValue(boundary, spatialObject);
        int partition = Arrays.binarySearch(partitionBounds, hValue);
        if (partition < 0) {
            partition = -partition - 1;
        }
        return partition;
    }

    private static int computeHValue(Envelope boundary, Envelope spatialObject)
    {
        int x = locationMapping(boundary.getMinX(), boundary.getMaxX(), (spatialObject.getMinX() + spatialObject.getMaxX()) / 2.0);
        int y = locationMapping(boundary.getMinY(), boundary.getMaxY(), (spatialObject.getMinY() + spatialObject.getMaxY()) / 2.0);
        return computeHValue(GRID_RESOLUTION + 1, x, y);
    }

    /**
     * Update envelope.
     *
     * @param envelope the envelope
     * @param spatialObject the spatial object
     * @return the envelope
     * @throws Exception the exception
     */
    public static Envelope updateEnvelope(Envelope envelope, Envelope spatialObject)
            throws Exception
    {
        double minX = Math.min(envelope.getMinX(), spatialObject.getMinX());
        double maxX = Math.max(envelope.getMaxX(), spatialObject.getMaxX());
        double minY = Math.min(envelope.getMinY(), spatialObject.getMinY());
        double maxY = Math.max(envelope.getMaxY(), spatialObject.getMaxY());

        return new Envelope(minX, maxX, minY, maxY);
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
