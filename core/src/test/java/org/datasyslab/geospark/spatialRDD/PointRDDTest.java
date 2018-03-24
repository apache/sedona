/**
 * FILE: PointRDDTest.java
 * PATH: org.datasyslab.geospark.spatialRDD.PointRDDTest.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialRDD;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.index.quadtree.Quadtree;
import com.vividsolutions.jts.index.strtree.STRtree;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

// TODO: Auto-generated Javadoc
/**
 * The Class PointRDDTest.
 */
public class PointRDDTest extends SpatialRDDTestBase
{
    /**
     * Once executed before all.
     */
    @BeforeClass
    public static void onceExecutedBeforeAll() {
        initialize(PointRDDTest.class.getSimpleName(), "point.test.properties");
    }

    /**
     * Test constructor.
     *
     * @throws Exception the exception
     */
    /*
        This test case will load a sample data file and
     */
    @Test
    public void testConstructor() throws Exception {
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter,true, numPartitions,StorageLevel.MEMORY_ONLY());
        assertEquals(inputCount, spatialRDD.approximateTotalCount);
        assertEquals(inputBoundary, spatialRDD.boundaryEnvelope);
    }

    /**
     * Test equal partitioning.
     *
     * @throws Exception the exception
     */
    /*
     *  This test case test whether the Hilbert Curve grid can be build correctly.
     */
    @Test
    public void testEqualPartitioning() throws Exception {
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, 10,StorageLevel.MEMORY_ONLY());
        spatialRDD.spatialPartitioning(GridType.EQUALGRID);
        for (Envelope d : spatialRDD.grids) {
        	//System.out.println("PointRDD spatial partitioning grids: "+d);
        }
        assert spatialRDD.countWithoutDuplicates()==spatialRDD.countWithoutDuplicatesSPRDD();
    }
    
    /**
     * Test hilbert curve spatial partitioing.
     *
     * @throws Exception the exception
     */
    /*
     *  This test case test whether the Hilbert Curve grid can be build correctly.
     */
    @Test
    public void testHilbertCurveSpatialPartitioing() throws Exception {
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true,10,StorageLevel.MEMORY_ONLY());
        spatialRDD.spatialPartitioning(GridType.HILBERT);
        for (Envelope d : spatialRDD.grids) {
        	//System.out.println("PointRDD spatial partitioning grids: "+d.grid);
        }
        assert spatialRDD.countWithoutDuplicates()==spatialRDD.countWithoutDuplicatesSPRDD();
    }
    
    /**
     * Test R tree spatial partitioing.
     *
     * @throws Exception the exception
     */
    /*
     *  This test case test whether the STR-Tree grid can be build correctly.
     */
    @Test
    public void testRTreeSpatialPartitioing() throws Exception {
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true,10,StorageLevel.MEMORY_ONLY());
        spatialRDD.spatialPartitioning(GridType.RTREE);
        for (Envelope d : spatialRDD.grids) {
        	//System.out.println("PointRDD spatial partitioning grids: "+d);
        }
        assert spatialRDD.countWithoutDuplicates()==spatialRDD.countWithoutDuplicatesSPRDD();
    }
    
    /**
     * Test voronoi spatial partitioing.
     *
     * @throws Exception the exception
     */
    /*
     *  This test case test whether the Voronoi grid can be build correctly.
     */
    @Test
    public void testVoronoiSpatialPartitioing() throws Exception {
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true,10,StorageLevel.MEMORY_ONLY());
        spatialRDD.spatialPartitioning(GridType.VORONOI);
        for (Envelope d : spatialRDD.grids) {
        	//System.out.println("PointRDD spatial partitioning grids: "+d.grid);
        }
        assert spatialRDD.countWithoutDuplicates()==spatialRDD.countWithoutDuplicatesSPRDD();
    }

    
    /**
     * Test build index without set grid.
     *
     * @throws Exception the exception
     */
    @Test
    public void testBuildIndexWithoutSetGrid() throws Exception {
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true,numPartitions,StorageLevel.MEMORY_ONLY());
        spatialRDD.buildIndex(IndexType.RTREE,false);
    }


    /**
     * Test build rtree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testBuildRtreeIndex() throws Exception {
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true,numPartitions,StorageLevel.MEMORY_ONLY());
        spatialRDD.spatialPartitioning(gridType);
        spatialRDD.buildIndex(IndexType.RTREE,true);
        if(spatialRDD.indexedRDD.take(1).get(0) instanceof STRtree)
        {
            List<Point> result = ((STRtree) spatialRDD.indexedRDD.take(1).get(0)).query(spatialRDD.boundaryEnvelope);
        }
        else
        {
            List<Point> result = ((Quadtree) spatialRDD.indexedRDD.take(1).get(0)).query(spatialRDD.boundaryEnvelope);

        }
        }
    
    /**
     * Test build quadtree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testBuildQuadtreeIndex() throws Exception {
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, numPartitions,StorageLevel.MEMORY_ONLY());
        spatialRDD.spatialPartitioning(gridType);
        spatialRDD.buildIndex(IndexType.QUADTREE,true);
        if(spatialRDD.indexedRDD.take(1).get(0) instanceof STRtree)
        {
            List<Point> result = ((STRtree) spatialRDD.indexedRDD.take(1).get(0)).query(spatialRDD.boundaryEnvelope);
        }
        else
        {
            List<Point> result = ((Quadtree) spatialRDD.indexedRDD.take(1).get(0)).query(spatialRDD.boundaryEnvelope);
        }
    }

    /**
     * Tear down.
     */
    @AfterClass
    public static void TearDown() {
        sc.stop();
    }
}