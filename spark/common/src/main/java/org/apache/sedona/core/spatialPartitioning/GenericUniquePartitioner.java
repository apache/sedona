package org.apache.sedona.core.spatialPartitioning;

import java.util.HashSet;
import java.util.Iterator;

import javax.annotation.Nullable;

import org.apache.sedona.core.joinJudgement.DedupParams;
import org.locationtech.jts.geom.Geometry;

import scala.Tuple2;

class GenericUniquePartitioner extends SpatialPartitioner {
    private SpatialPartitioner parent;

    protected GenericUniquePartitioner(SpatialPartitioner parent) {
        super(parent.getGridType(), parent.getGrids());
        this.parent = parent;
    }

    @Override
    public Iterator<Tuple2<Integer, Geometry>> placeObject(Geometry spatialObject) throws Exception {
        Iterator<Tuple2<Integer, Geometry>> it = parent.placeObject(spatialObject);
        int minParitionId = Integer.MAX_VALUE;
        Geometry minGeometry = null;
        while (it.hasNext()) {
            Tuple2<Integer, Geometry> value = it.next();
            if (value._1() < minParitionId) {
                minParitionId = value._1();
                minGeometry = value._2();
            }
        }

        HashSet<Tuple2<Integer, Geometry>> out = new HashSet<Tuple2<Integer, Geometry>>();
        if (minGeometry != null) {
            out.add(new Tuple2<Integer, Geometry>(minParitionId, minGeometry));
        }

        return out.iterator();
    }

    @Override
    @Nullable
    public DedupParams getDedupParams() {
        throw new UnsupportedOperationException("Unique partitioner cannot deduplicate join results");
    }

    @Override
    public int numPartitions() {
        return parent.numPartitions();
    }

}
