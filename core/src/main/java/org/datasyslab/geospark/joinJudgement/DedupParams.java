/**
 * FILE: DedupParams.java
 * PATH: org.datasyslab.geospark.joinJudgement.DedupParams.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.joinJudgement;

import com.vividsolutions.jts.geom.Envelope;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * Contains information necessary to activate de-dup logic in sub-classes of {@link JudgementBase}.
 */
public final class DedupParams implements Serializable {
    private final List<Envelope> partitionExtents;

    /**
     * @param partitionExtents A list of partition extents in such an order that
     *                         an index of an element in this list matches partition ID.
     */
    public DedupParams(List<Envelope> partitionExtents) {
        this.partitionExtents = Objects.requireNonNull(partitionExtents, "partitionExtents");
    }

    public List<Envelope> getPartitionExtents() {
        return partitionExtents;
    }
}
