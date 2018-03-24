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

public class LeftIndexLookupJudgement<T extends Geometry, U extends Geometry>
        extends JudgementBase
        implements FlatMapFunction2<Iterator<SpatialIndex>, Iterator<U>, Pair<T, U>>, Serializable
{

    /**
     * @see JudgementBase
     */
    public LeftIndexLookupJudgement(boolean considerBoundaryIntersection, @Nullable DedupParams dedupParams)
    {
        super(considerBoundaryIntersection, dedupParams);
    }

    @Override
    public Iterator<Pair<T, U>> call(Iterator<SpatialIndex> indexIterator, Iterator<U> streamShapes)
            throws Exception
    {
        List<Pair<T, U>> result = new ArrayList<>();

        if (!indexIterator.hasNext() || !streamShapes.hasNext()) {
            return result.iterator();
        }

        initPartition();

        SpatialIndex treeIndex = indexIterator.next();
        while (streamShapes.hasNext()) {
            U streamShape = streamShapes.next();
            List<Geometry> candidates = treeIndex.query(streamShape.getEnvelopeInternal());
            for (Geometry candidate : candidates) {
                // Refine phase. Use the real polygon (instead of its MBR) to recheck the spatial relation.
                if (match(candidate, streamShape)) {
                    result.add(Pair.of((T) candidate, streamShape));
                }
            }
        }
        return result.iterator();
    }
}
