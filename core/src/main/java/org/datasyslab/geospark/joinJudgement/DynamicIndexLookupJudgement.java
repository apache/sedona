package org.datasyslab.geospark.joinJudgement;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.index.SpatialIndex;
import com.vividsolutions.jts.index.quadtree.Quadtree;
import com.vividsolutions.jts.index.strtree.STRtree;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.datasyslab.geospark.enums.IndexType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class DynamicIndexLookupJudgement<T extends Geometry, U extends Geometry>
        extends JudgementBase
        implements FlatMapFunction2<Iterator<T>, Iterator<U>, Pair<U, T>>, Serializable {

    private final IndexType indexType;

    public DynamicIndexLookupJudgement(boolean considerBoundaryIntersection, IndexType indexType) {
        super(considerBoundaryIntersection);
        this.indexType = indexType;
    }

    @Override
    public Iterator<Pair<U, T>> call(final Iterator<T> shapes, final Iterator<U> windowShapes) throws Exception {

        if (!windowShapes.hasNext()) {
            return Collections.emptyIterator();
        }

        final SpatialIndex spatialIndex = buildIndex(windowShapes);

        return new Iterator<Pair<U, T>>() {
            // A batch of pre-computed matches
            private List<Pair<U, T>> batch = null;
            // An index of the element from 'batch' to return next
            private int nextIndex = 0;

            @Override
            public boolean hasNext() {
                if (batch != null) {
                    return true;
                } else {
                    return populateNextBatch();
                }
            }

            @Override
            public Pair<U, T> next() {
                if (batch == null) {
                    populateNextBatch();
                }

                if (batch != null) {
                    final Pair<U, T> result = batch.get(nextIndex);
                    nextIndex++;
                    if (nextIndex >= batch.size()) {
                        populateNextBatch();
                        nextIndex = 0;
                    }
                    return result;
                }

                throw new NoSuchElementException();
            }

            private boolean populateNextBatch() {
                if (!shapes.hasNext()) {
                    if (batch != null) {
                        batch = null;
                    }
                    return false;
                }

                batch = new ArrayList<>();

                while (shapes.hasNext()) {
                    final T shape = shapes.next();
                    final List candidates = spatialIndex.query(shape.getEnvelopeInternal());
                    for (Object candidate : candidates) {
                        final U polygon = (U) candidate;
                        if (match(polygon, shape)) {
                            batch.add(Pair.of(polygon, shape));
                        }
                    }
                    if (!batch.isEmpty()) {
                        return true;
                    }
                }

                batch = null;
                return false;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private SpatialIndex buildIndex(Iterator<U> polygons) {
        final SpatialIndex index = newIndex();
        while (polygons.hasNext()) {
            U polygon = polygons.next();
            index.insert(polygon.getEnvelopeInternal(), polygon);
        }
        index.query(new Envelope(0.0,0.0,0.0,0.0));
        return index;
    }

    private SpatialIndex newIndex() {
        switch (indexType) {
            case RTREE:
                return new STRtree();
            case QUADTREE:
                return new Quadtree();
            default:
                throw new IllegalArgumentException("Unsupported index type: " + indexType);
        }
    }
}
