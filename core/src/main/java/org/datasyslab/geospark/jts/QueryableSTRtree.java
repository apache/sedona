package org.datasyslab.geospark.jts;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.index.strtree.AbstractNode;
import com.vividsolutions.jts.index.strtree.Boundable;
import com.vividsolutions.jts.index.strtree.STRtree;

import java.util.ArrayList;
import java.util.List;

/**
 * A class that adds query bounds to STRtree
 */
public class QueryableSTRtree extends STRtree {
    public QueryableSTRtree(int nodeCapacity) {
        super(nodeCapacity);
    }

    public List<Envelope> queryBoundary() {
        build();
        List<Envelope> boundaries = new ArrayList();
        queryBoundary(root, boundaries);
        return boundaries;
    }

    private void queryBoundary(AbstractNode node, List<Envelope> matches) {
        List childBoundables = node.getChildBoundables();
        if (node.getLevel() == 0) {
            matches.add((Envelope)node.getBounds());
        } else {
            for (int i = 0; i < childBoundables.size(); i++) {
                Boundable childBoundable = (Boundable) childBoundables.get(i);
                if (childBoundable instanceof AbstractNode) {
                    queryBoundary((AbstractNode) childBoundable, matches);
                }
            }
        }
    }
}
