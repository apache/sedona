/*
 * FILE: PolygonRDDTest
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package org.datasyslab.geospark.spatialRDD;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.quadtree.Quadtree;
import com.vividsolutions.jts.index.strtree.STRtree;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

// TODO: Auto-generated Javadoc

/**
 * The Class PolygonRDDTest.
 */
public class PolygonRDDTest
        extends SpatialRDDTestBase
{
    private static String InputLocationGeojson;

    /**
     * Once executed before all.
     */
    @BeforeClass
    public static void onceExecutedBeforeAll()
    {
        initialize(PolygonRDDTest.class.getSimpleName(), "polygon.test.properties");
        InputLocationGeojson = "file://" + PolygonRDDTest.class.getClassLoader().getResource(prop.getProperty("inputLocationGeojson")).getPath();
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
        PolygonRDD spatialRDD = new PolygonRDD(sc, InputLocation, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY());
        assertEquals(inputCount, spatialRDD.approximateTotalCount);
        assertEquals(inputBoundary, spatialRDD.boundaryEnvelope);
    }

    @Test
    public void testEmptyConstructor()
            throws Exception
    {
        PolygonRDD spatialRDD = new PolygonRDD(sc, InputLocation, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY());
        spatialRDD.spatialPartitioning(gridType);
        spatialRDD.buildIndex(IndexType.RTREE, true);
        // Create an empty spatialRDD and manually assemble it
        PolygonRDD spatialRDDcopy = new PolygonRDD();
        spatialRDDcopy.rawSpatialRDD = spatialRDD.rawSpatialRDD;
        spatialRDDcopy.indexedRawRDD = spatialRDD.indexedRawRDD;
        spatialRDDcopy.analyze();
    }

    @Test
    public void testGeoJSONConstructor()
            throws Exception
    {
        PolygonRDD spatialRDD = new PolygonRDD(sc, InputLocationGeojson, FileDataSplitter.GEOJSON, true, 4, StorageLevel.MEMORY_ONLY());
        //todo: Set this to debug level
        assert spatialRDD.approximateTotalCount == 1001;
        assert spatialRDD.boundaryEnvelope != null;
    }

    /**
     * Test hilbert curve spatial partitioing.
     *
     * @throws Exception the exception
     */
    @Test
    public void testHilbertCurveSpatialPartitioing()
            throws Exception
    {
        PolygonRDD spatialRDD = new PolygonRDD(sc, InputLocation, splitter, true, 10, StorageLevel.MEMORY_ONLY());
        spatialRDD.spatialPartitioning(GridType.HILBERT);
        for (Envelope d : spatialRDD.grids) {
            //System.out.println("PointRDD spatial partitioning grids: "+d.grid);
        }
    }

    /**
     * Test R tree spatial partitioing.
     *
     * @throws Exception the exception
     */
    @Test
    public void testRTreeSpatialPartitioing()
            throws Exception
    {
        PolygonRDD spatialRDD = new PolygonRDD(sc, InputLocation, splitter, true, 10, StorageLevel.MEMORY_ONLY());
        spatialRDD.spatialPartitioning(GridType.RTREE);
        for (Envelope d : spatialRDD.grids) {
            //System.out.println("PointRDD spatial partitioning grids: "+d.grid);
        }
    }

    /**
     * Test voronoi spatial partitioing.
     *
     * @throws Exception the exception
     */
    @Test
    public void testVoronoiSpatialPartitioing()
            throws Exception
    {
        PolygonRDD spatialRDD = new PolygonRDD(sc, InputLocation, splitter, true, 10, StorageLevel.MEMORY_ONLY());
        spatialRDD.spatialPartitioning(GridType.VORONOI);
        for (Envelope d : spatialRDD.grids) {
            //System.out.println("PointRDD spatial partitioning grids: "+d.grid);
        }
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
        PolygonRDD spatialRDD = new PolygonRDD(sc, InputLocation, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY());
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
        PolygonRDD spatialRDD = new PolygonRDD(sc, InputLocation, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY());
        spatialRDD.spatialPartitioning(gridType);
        spatialRDD.buildIndex(IndexType.RTREE, true);
        if (spatialRDD.indexedRDD.take(1).get(0) instanceof STRtree) {
            List<Polygon> result = ((STRtree) spatialRDD.indexedRDD.take(1).get(0)).query(spatialRDD.boundaryEnvelope);
        }
        else {
            List<Polygon> result = ((Quadtree) spatialRDD.indexedRDD.take(1).get(0)).query(spatialRDD.boundaryEnvelope);
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
        PolygonRDD spatialRDD = new PolygonRDD(sc, InputLocation, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY());
        spatialRDD.spatialPartitioning(gridType);
        spatialRDD.buildIndex(IndexType.QUADTREE, true);
        if (spatialRDD.indexedRDD.take(1).get(0) instanceof STRtree) {
            List<Polygon> result = ((STRtree) spatialRDD.indexedRDD.take(1).get(0)).query(spatialRDD.boundaryEnvelope);
        }
        else {
            List<Polygon> result = ((Quadtree) spatialRDD.indexedRDD.take(1).get(0)).query(spatialRDD.boundaryEnvelope);
        }
    }

    /**
     * Test MBR.
     *
     * @throws Exception the exception
     */
    @Test
    public void testMBR()
            throws Exception
    {
        PolygonRDD polygonRDD = new PolygonRDD(sc, InputLocation, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY());
        RectangleRDD rectangleRDD = polygonRDD.MinimumBoundingRectangle();
        List<Polygon> result = rectangleRDD.rawSpatialRDD.collect();
        assert result.size() > -1;
    }  
    
    /*
    @Test
    public void testPolygonUnion()
    {
    	PolygonRDD polygonRDD = new PolygonRDD(sc, InputLocation, offset, splitter, numPartitions);
    	assert polygonRDD.PolygonUnion() instanceof Polygon;
    }
    */

    /**
     * Tear down.
     */
    @AfterClass
    public static void TearDown()
    {
        sc.stop();
    }
}
