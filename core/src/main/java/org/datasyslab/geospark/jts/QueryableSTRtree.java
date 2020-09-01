/*
 * FILE: QueryableSTRtree
 * Copyright (c) 2015 - 2019 GeoSpark Development Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.datasyslab.geospark.jts;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.index.strtree.AbstractNode;
import org.locationtech.jts.index.strtree.Boundable;
import org.locationtech.jts.index.strtree.STRtree;

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
