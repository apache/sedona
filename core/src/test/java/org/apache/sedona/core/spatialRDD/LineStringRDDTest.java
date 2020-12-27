/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sedona.core.spatialRDD;

import org.apache.sedona.core.enums.IndexType;
import org.apache.spark.storage.StorageLevel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.index.strtree.STRtree;

import java.util.List;

import static org.junit.Assert.assertEquals;

// TODO: Auto-generated Javadoc

/**
 * The Class LineStringRDDTest.
 */
public class LineStringRDDTest
        extends SpatialRDDTestBase
{

    /**
     * Once executed before all.
     */
    @BeforeClass
    public static void onceExecutedBeforeAll()
    {
        initialize(LineStringRDDTest.class.getSimpleName(), "linestring.test.properties");
    }

    /**
     * Tear down.
     */
    @AfterClass
    public static void TearDown()
    {
        sc.stop();
    }

    /**
     * Test constructor.
     *
     * @throws Exception the exception
     */
    @Test
    public void testConstructor()
            throws Exception
    {
        LineStringRDD spatialRDD = new LineStringRDD(sc, InputLocation, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY());
        assertEquals(inputCount, spatialRDD.approximateTotalCount);
        assertEquals(inputBoundary, spatialRDD.boundaryEnvelope);
    }

    @Test
    public void testEmptyConstructor()
            throws Exception
    {
        LineStringRDD spatialRDD = new LineStringRDD(sc, InputLocation, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY());
        spatialRDD.spatialPartitioning(gridType);
        spatialRDD.buildIndex(IndexType.RTREE, true);
        // Create an empty spatialRDD and manually assemble it
        LineStringRDD spatialRDDcopy = new LineStringRDD();
        spatialRDDcopy.rawSpatialRDD = spatialRDD.rawSpatialRDD;
        spatialRDDcopy.indexedRawRDD = spatialRDD.indexedRawRDD;
        spatialRDDcopy.analyze();
    }

    /**
     * Test build index without set grid.
     *
     * @throws Exception the exception
     */
    @Test
    public void testBuildIndexWithoutSetGrid()
            throws Exception
    {
        LineStringRDD spatialRDD = new LineStringRDD(sc, InputLocation, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY());
        spatialRDD.buildIndex(IndexType.RTREE, false);
    }

    /**
     * Test build rtree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testBuildRtreeIndex()
            throws Exception
    {
        LineStringRDD spatialRDD = new LineStringRDD(sc, InputLocation, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY());
        spatialRDD.spatialPartitioning(gridType);
        spatialRDD.buildIndex(IndexType.RTREE, true);
        if (spatialRDD.indexedRDD.take(1).get(0) instanceof STRtree) {
            List<Polygon> result = spatialRDD.indexedRDD.take(1).get(0).query(spatialRDD.boundaryEnvelope);
        }
        else {
            List<Polygon> result = spatialRDD.indexedRDD.take(1).get(0).query(spatialRDD.boundaryEnvelope);
        }
    }

    /**
     * Test build quadtree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testBuildQuadtreeIndex()
            throws Exception
    {
        LineStringRDD spatialRDD = new LineStringRDD(sc, InputLocation, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY());
        spatialRDD.spatialPartitioning(gridType);
        spatialRDD.buildIndex(IndexType.QUADTREE, true);
        if (spatialRDD.indexedRDD.take(1).get(0) instanceof STRtree) {
            List<Polygon> result = spatialRDD.indexedRDD.take(1).get(0).query(spatialRDD.boundaryEnvelope);
        }
        else {
            List<Polygon> result = spatialRDD.indexedRDD.take(1).get(0).query(spatialRDD.boundaryEnvelope);
        }
    }
    
    /*
    @Test
    public void testPolygonUnion()
    {
    	LineStringRDD lineStringRDD = new LineStringRDD(sc, InputLocation, offset, splitter, numPartitions);
    	assert lineStringRDD.PolygonUnion() instanceof Polygon;
    }
    */

    /**
     * Test MBR.
     *
     * @throws Exception the exception
     */
    @Test
    public void testMBR()
            throws Exception
    {
        LineStringRDD lineStringRDD = new LineStringRDD(sc, InputLocation, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY());
        RectangleRDD rectangleRDD = lineStringRDD.MinimumBoundingRectangle();
        List<Polygon> result = rectangleRDD.rawSpatialRDD.collect();
        assert result.size() > -1;
    }
}
