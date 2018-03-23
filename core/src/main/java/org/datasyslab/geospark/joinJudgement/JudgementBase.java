/*
 * FILE: JudgementBase
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
package org.datasyslab.geospark.joinJudgement;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.TaskContext;
import org.datasyslab.geospark.utils.HalfOpenRectangle;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;

/**
 * Base class for partition level join implementations.
 * <p>
 * Provides `match` method to test whether a given pair of geometries satisfies join condition.
 * <p>
 * Supports 'contains' and 'intersects' join conditions.
 * <p>
 * Provides optional de-dup logic. Due to the nature of spatial partitioning, the same pair of
 * geometries may appear in multiple partitions. If that pair satisfies join condition, it
 * will be included in join results multiple times. This duplication can be avoided by
 * (1) choosing spatial partitioning that doesn't allow for overlapping partition extents
 * and (2) reporting a pair of matching geometries only from the partition
 * whose extent contains the reference point of the intersection of the geometries.
 * <p>
 * To achieve (1), call SpatialRDD.spatialPartitioning with a GridType.QUADTREE. At the moment
 * this is the only grid type supported by de-dup logic.
 * <p>
 * For (2), provide `DedupParams` when instantiating JudgementBase object. If `DedupParams`
 * is specified, the implementation of the `match` method assumes that condition (1) holds.
 */
abstract class JudgementBase
        implements Serializable
{
    private static final Logger log = LogManager.getLogger(JudgementBase.class);

    private final boolean considerBoundaryIntersection;
    private final DedupParams dedupParams;

    transient private HalfOpenRectangle extent;

    /**
     * @param considerBoundaryIntersection true for 'intersects', false for 'contains' join condition
     * @param dedupParams Optional information to activate de-dup logic
     */
    protected JudgementBase(boolean considerBoundaryIntersection, @Nullable DedupParams dedupParams)
    {
        this.considerBoundaryIntersection = considerBoundaryIntersection;
        this.dedupParams = dedupParams;
    }

    /**
     * Looks up the extent of the current partition. If found, `match` method will
     * activate the logic to avoid emitting duplicate join results from multiple partitions.
     * <p>
     * Must be called before processing a partition. Must be called from the
     * same instance that will be used to process the partition.
     */
    protected void initPartition()
    {
        if (dedupParams == null) {
            return;
        }

        final int partitionId = TaskContext.getPartitionId();

        final List<Envelope> partitionExtents = dedupParams.getPartitionExtents();
        if (partitionId < partitionExtents.size()) {
            extent = new HalfOpenRectangle(partitionExtents.get(partitionId));
        }
        else {
            log.warn("Didn't find partition extent for this partition: " + partitionId);
        }
    }

    protected boolean match(Geometry left, Geometry right)
    {
        if (extent != null) {
            // Handle easy case: points. Since each point is assigned to exactly one partition,
            // different partitions cannot emit duplicate results.
            if (left instanceof Point || right instanceof Point) {
                return geoMatch(left, right);
            }

            // Neither geometry is a point

            // Check if reference point of the intersection of the bounding boxes lies within
            // the extent of this partition. If not, don't run any checks. Let the partition
            // that contains the reference point do all the work.
            Envelope intersection =
                    left.getEnvelopeInternal().intersection(right.getEnvelopeInternal());
            if (!intersection.isNull()) {
                final Point referencePoint =
                        makePoint(intersection.getMinX(), intersection.getMinY(), left.getFactory());
                if (!extent.contains(referencePoint)) {
                    return false;
                }
            }
        }

        return geoMatch(left, right);
    }

    private Point makePoint(double x, double y, GeometryFactory factory)
    {
        return factory.createPoint(new Coordinate(x, y));
    }

    private boolean geoMatch(Geometry left, Geometry right)
    {
        //log.warn("Check "+left.toText()+" with "+right.toText());
        return considerBoundaryIntersection ? left.intersects(right) : left.covers(right);
    }
}
