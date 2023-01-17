package org.apache.sedona.core.joinJudgement;

import org.apache.commons.collections.iterators.FilterIterator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.sedona.common.utils.GeomUtils;
import org.apache.sedona.common.utils.HalfOpenRectangle;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.locationtech.jts.geom.*;

import java.util.Iterator;
import java.util.List;

/**
 * Provides optional de-dup logic. Due to the nature of spatial partitioning, the same pair of
 * geometries may appear in multiple partitions. If that pair satisfies join condition, it
 * will be included in join results multiple times. This duplication can be avoided by
 * (1) choosing spatial partitioning that doesn't allow for overlapping partition extents
 * and (2) reporting a pair of matching geometries only from the partition
 * whose extent contains the reference point of the intersection of the geometries.
 *
 * @param <U>
 * @param <T>
 */
public class DuplicatesFilter<U extends Geometry, T extends Geometry> implements Function2<Integer, Iterator<Pair<U, T>>, Iterator<Pair<U, T>>> {

    private static final Logger log = LogManager.getLogger(DuplicatesFilter.class);
    private final Broadcast<DedupParams> dedupParamsBroadcast;

    public DuplicatesFilter(Broadcast<DedupParams> dedupParamsBroadcast) {
        this.dedupParamsBroadcast = dedupParamsBroadcast;
    }

    @Override
    public Iterator<Pair<U, T>> call(Integer partitionId, Iterator<Pair<U, T>> geometryPair) throws Exception {
        final List<Envelope> partitionExtents = dedupParamsBroadcast.getValue().getPartitionExtents();
        if (partitionId < partitionExtents.size()) {
            HalfOpenRectangle extent = new HalfOpenRectangle(partitionExtents.get(partitionId));
            return new FilterIterator(geometryPair, p -> !GeomUtils.isDuplicate(((Pair<U, T>) p).getLeft(), ((Pair<U, T>) p).getRight(), extent));
        }
        else {
            log.warn("Didn't find partition extent for this partition: " + partitionId);
            return geometryPair;
        }
    }
}
