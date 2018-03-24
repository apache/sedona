package org.datasyslab.geospark.joinJudgement;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.index.SpatialIndex;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.api.java.function.FlatMapFunction2;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RightIndexLookupJudgement<T extends Geometry, U extends Geometry>
    extends JudgementBase
    implements FlatMapFunction2<Iterator<T>, Iterator<SpatialIndex>, Pair<T, U>>, Serializable {

    /**
     * @see JudgementBase
     */
    public RightIndexLookupJudgement(boolean considerBoundaryIntersection, @Nullable DedupParams dedupParams) {
        super(considerBoundaryIntersection, dedupParams);
    }

    @Override
    public Iterator<Pair<T, U>> call(Iterator<T> streamShapes, Iterator<SpatialIndex> indexIterator) throws Exception {
        List<Pair<T, U>> result = new ArrayList<>();

        if (!indexIterator.hasNext() || !streamShapes.hasNext()) {
            return result.iterator();
        }

        initPartition();

        SpatialIndex treeIndex = indexIterator.next();
        while (streamShapes.hasNext()) {
            T streamShape = streamShapes.next();
            List<Geometry> candidates = treeIndex.query(streamShape.getEnvelopeInternal());
            for (Geometry candidate : candidates) {
                // Refine phase. Use the real polygon (instead of its MBR) to recheck the spatial relation.
                if (match(streamShape, candidate)) {
                    result.add(Pair.of(streamShape, (U) candidate));
                }
            }
        }
        return result.iterator();
    }
}
