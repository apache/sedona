/**
 * FILE: IndexLookupJudgement.java
 * PATH: org.datasyslab.geospark.joinJudgement.IndexLookupJudgement.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.joinJudgement;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.SpatialIndex;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.api.java.function.FlatMapFunction2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class IndexLookupJudgement<T extends Geometry>
        extends JudgementBase
        implements FlatMapFunction2<Iterator<SpatialIndex>, Iterator<Polygon>, Pair<Polygon, T>>, Serializable {

    /**
     * Instantiates a new geometry by polygon judgement using index.
     *
     * @param considerBoundaryIntersection the consider boundary intersection
     */
    public IndexLookupJudgement(boolean considerBoundaryIntersection) {
        super(considerBoundaryIntersection);
    }

    @Override
    public Iterator<Pair<Polygon, T>> call(Iterator<SpatialIndex> iteratorTree, Iterator<Polygon> iteratorWindow) throws Exception {
        List<Pair<Polygon, T>> result = new ArrayList<>();

        if (!iteratorTree.hasNext()) {
            return result.iterator();
        }

        SpatialIndex treeIndex = iteratorTree.next();
        while (iteratorWindow.hasNext()) {
            Polygon window = iteratorWindow.next();
            List<Geometry> queryResult = treeIndex.query(window.getEnvelopeInternal());
            if (queryResult.size() == 0) continue;
            for (Geometry spatialObject : queryResult) {
                // Refine phase. Use the real polygon (instead of its MBR) to recheck the spatial relation.
                if (match(window, spatialObject)) {
                    result.add(Pair.of(window, (T) spatialObject));
                }
            }
        }
        return result.iterator();
    }
}
