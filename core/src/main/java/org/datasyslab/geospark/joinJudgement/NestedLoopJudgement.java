/**
 * FILE: NestedLoopJudgement.java
 * PATH: org.datasyslab.geospark.joinJudgement.NestedLoopJudgement.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.joinJudgement;

import com.vividsolutions.jts.geom.Geometry;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.api.java.function.FlatMapFunction2;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class NestedLoopJudgement<T extends Geometry, U extends Geometry>
        extends JudgementBase
        implements FlatMapFunction2<Iterator<T>, Iterator<U>, Pair<U, T>>, Serializable {

    /**
     * @see JudgementBase
     */
    public NestedLoopJudgement(boolean considerBoundaryIntersection, @Nullable DedupParams dedupParams) {
        super(considerBoundaryIntersection, dedupParams);
    }

    @Override
    public Iterator<Pair<U, T>> call(Iterator<T> iteratorObject, Iterator<U> iteratorWindow) throws Exception {
        initPartition();

        List<Pair<U, T>> result = new ArrayList<>();
        List<T> queryObjects = new ArrayList<>();
        while(iteratorObject.hasNext())
        {
            queryObjects.add(iteratorObject.next());
        }
        while (iteratorWindow.hasNext()) {
            U window = iteratorWindow.next();
            for (int i =0;i<queryObjects.size();i++) {
                T object = queryObjects.get(i);
                if (match(window, object)) {
                    result.add(Pair.of(window, object));
                }
            }
        }
        return result.iterator();
    }
}
