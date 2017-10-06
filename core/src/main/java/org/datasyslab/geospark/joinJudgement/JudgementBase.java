package org.datasyslab.geospark.joinJudgement;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.TaskContext;
import org.datasyslab.geospark.utils.HalfOpenRectangle;
import org.datasyslab.geospark.utils.ReferencePointUtils;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;

/**
 * Base class for partition level join implementations.
 *
 * Provides `match` method to test whether a given pair of geometries satisfies join condition.
 *
 * Supports 'contains' and 'intersects' join conditions.
 *
 * Provides optional de-dup logic. Due to the nature of spatial partitioning, the same pair of
 * geometries may appear in multiple partitions. If that pair satisfies join condition, it
 * will be included in join results multiple times. This duplication can be avoided by
 * (1) choosing spatial partitioning that doesn't allow for overlapping partition extents
 * and (2) reporting a pair of matching geometries only from the partition
 * whose extent contains the reference point of the intersection of the geometries.
 *
 * To achieve (1), call SpatialRDD.spatialPartitioning with a GridType.QUADTREE. At the moment
 * this is the only grid type supported by de-dup logic.
 *
 * For (2), provide `DedupParams` when instantiating JudgementBase object. If `DedupParams`
 * is specified, the implementation of the `match` method assumes that condition (1) holds.
 */
abstract class JudgementBase implements Serializable {
    private static final Logger log = LogManager.getLogger(DynamicIndexLookupJudgement.class);

    private final boolean considerBoundaryIntersection;
    private final DedupParams dedupParams;

    transient private HalfOpenRectangle extent;

    /**
     * @param considerBoundaryIntersection true for 'intersects', false for 'contains' join condition
     * @param dedupParams Optional information to activate de-dup logic
     */
    protected JudgementBase(boolean considerBoundaryIntersection, @Nullable DedupParams dedupParams) {
        this.considerBoundaryIntersection = considerBoundaryIntersection;
        this.dedupParams = dedupParams;
    }

    /**
     * Looks up the extent of the current partition. If found, `match` method will
     * activate the logic to avoid emitting duplicate join results from multiple partitions.
     *
     * Must be called before processing a partition. Must be called from the
     * same instance that will be used to process the partition.
     */
    protected void initPartition() {
        if (dedupParams == null) {
            return;
        }

        final int partitionId = TaskContext.getPartitionId();

        final List<Envelope> partitionExtents = dedupParams.getPartitionExtents();
        if (partitionId < partitionExtents.size()) {
            extent = new HalfOpenRectangle(partitionExtents.get(partitionId));
        } else {
            log.warn("Didn't find partition extent for this partition: " + partitionId);
        }
    }

    protected boolean match(Geometry left, Geometry right) {
        if (!geoMatch(left, right)) {
            return false;
        }

        if (extent == null) {
            return true;
        }

        // Handle easy case: points. Since each point is assigned to exactly one partition,
        // different partitions cannot emit duplicate results.
        if (left instanceof Point || right instanceof Point) {
            return true;
        }

        // For more complex geometries, check if reference point of the intersection lies
        // within the extent of this partition.
        final Geometry intersection = left.intersection(right);
        final Point referencePoint = ReferencePointUtils.getReferencePoint(intersection);
        return extent.contains(referencePoint);
    }

    private boolean geoMatch(Geometry left, Geometry right) {
        return considerBoundaryIntersection ? left.intersects(right) : left.covers(right);
    }
}
