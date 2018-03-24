package org.datasyslab.geospark.spatialPartitioning.quadtree;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import org.datasyslab.geospark.spatialPartitioning.QuadtreePartitioning;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class QuadTreePartitioningTest
{

    private final GeometryFactory factory = new GeometryFactory();

    /**
     * Verifies that data skew doesn't cause java.lang.StackOverflowError
     * in StandardQuadTree.insert
     */
    @Test
    public void testDataSkew()
            throws Exception
    {

        // Create an artificially skewed data set of identical envelopes
        final Point point = factory.createPoint(new Coordinate(0, 0));

        final List<Envelope> samples = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            samples.add(point.getEnvelopeInternal());
        }

        final Envelope extent = new Envelope(0, 1, 0, 1);

        // Make sure Quad-tree is built successfully without throwing
        // java.lang.StackOverflowError
        QuadtreePartitioning partitioning = new QuadtreePartitioning(samples, extent, 10);
        Assert.assertNotNull(partitioning.getPartitionTree());
    }
}
