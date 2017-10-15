package org.datasyslab.geospark.spatialRddTool;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.index.SpatialIndex;
import com.vividsolutions.jts.index.quadtree.Quadtree;
import com.vividsolutions.jts.index.strtree.STRtree;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.datasyslab.geospark.enums.IndexType;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public final class IndexBuilder<T extends Geometry> implements FlatMapFunction<Iterator<T>, SpatialIndex> {
    IndexType indexType;
    public IndexBuilder(IndexType indexType)
    {
        this.indexType = indexType;
    }
    @Override
    public Iterator<SpatialIndex> call(Iterator<T> objectIterator) throws Exception {
        SpatialIndex spatialIndex;
        if (indexType == IndexType.RTREE) {
            spatialIndex = new STRtree();
        }
        else
        {
            spatialIndex = new Quadtree();
        }
        while (objectIterator.hasNext()) {
            T spatialObject = objectIterator.next();
            spatialIndex.insert(spatialObject.getEnvelopeInternal(), spatialObject);
        }
        Set<SpatialIndex> result = new HashSet();
        spatialIndex.query(new Envelope(0.0, 0.0, 0.0, 0.0));
        result.add(spatialIndex);
        return result.iterator();
    }
}
