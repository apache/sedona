package org.datasyslab.geospark.spatialPartitioning;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.joinJudgement.DedupParams;
import org.datasyslab.geospark.utils.HalfOpenRectangle;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class KDBTreePartitioner extends SpatialPartitioner {
    private final KDBTree tree;

    public KDBTreePartitioner(KDBTree tree) {
        super(GridType.KDBTREE, getLeafZones(tree));
        this.tree = tree;
        this.tree.dropElements();
    }

    @Override
    public int numPartitions() {
        return grids.size();
    }

    @Override
    public <T extends Geometry> Iterator<Tuple2<Integer, T>> placeObject(T spatialObject)
        throws Exception {

        Objects.requireNonNull(spatialObject, "spatialObject");

        final Envelope envelope = spatialObject.getEnvelopeInternal();

        final List<KDBTree> matchedPartitions = tree.findLeafNodes(envelope);

        final Point point = spatialObject instanceof Point ? (Point) spatialObject : null;

        final Set<Tuple2<Integer, T>> result = new HashSet<>();
        for (KDBTree leaf : matchedPartitions) {
            // For points, make sure to return only one partition
            if (point != null && !(new HalfOpenRectangle(leaf.getExtent())).contains(point)) {
                continue;
            }

            result.add(new Tuple2(leaf.getLeafId(), spatialObject));
        }

        return result.iterator();
    }

    @Nullable
    @Override
    public DedupParams getDedupParams() {
        return new DedupParams(grids);
    }

    private static List<Envelope> getLeafZones(KDBTree tree) {
        final List<Envelope> leafs = new ArrayList<>();
        tree.traverse(new KDBTree.Visitor() {
            @Override
            public boolean visit(KDBTree tree) {
                if (tree.isLeaf()) {
                    leafs.add(tree.getExtent());
                }
                return true;
            }
        });

        return leafs;
    }
}
