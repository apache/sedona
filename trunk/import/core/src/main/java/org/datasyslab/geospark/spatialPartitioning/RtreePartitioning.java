/*
 * FILE: RtreePartitioning
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
package org.datasyslab.geospark.spatialPartitioning;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.index.strtree.STRtree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

// TODO: Auto-generated Javadoc

/**
 * The Class RtreePartitioning.
 */
public class RtreePartitioning
        implements Serializable
{

    /**
     * The grids.
     */
    final List<Envelope> grids = new ArrayList<>();

    /**
     * Instantiates a new rtree partitioning.
     *
     * @param samples the sample list
     * @param partitions the partitions
     * @throws Exception the exception
     */
    public RtreePartitioning(List<Envelope> samples, int partitions)
            throws Exception
    {
        STRtree strtree = new STRtree(samples.size() / partitions);
        for (Envelope sample : samples) {
            strtree.insert(sample, sample);
        }

        List<Envelope> envelopes = strtree.queryBoundary();
        for (Envelope envelope : envelopes) {
            grids.add(envelope);
        }
    }

    /**
     * Gets the grids.
     *
     * @return the grids
     */
    public List<Envelope> getGrids()
    {

        return this.grids;
    }
}
