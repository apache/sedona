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

import org.apache.sedona.common.enums.FileDataSplitter;
import org.apache.sedona.core.enums.IndexType;
import org.apache.spark.storage.StorageLevel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.index.strtree.STRtree;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
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
    private static String InputLocationWkt;
    private static String InputLocationWkb;

    /**
     * Once executed before all.
     */
    @BeforeClass
    public static void onceExecutedBeforeAll()
    {
        initialize(PolygonRDDTest.class.getSimpleName(), "polygon.test.properties");
        InputLocationGeojson = "file://" + PolygonRDDTest.class.getClassLoader().getResource(prop.getProperty("inputLocationGeojson")).getPath();
        InputLocationWkt = "file://" + PolygonRDDTest.class.getClassLoader().getResource(prop.getProperty("inputLocationWkt")).getPath();
        InputLocationWkb = "file://" + PolygonRDDTest.class.getClassLoader().getResource(prop.getProperty("inputLocationWkb")).getPath();
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
    {
        PolygonRDD spatialRDD = new PolygonRDD(sc, InputLocationGeojson, FileDataSplitter.GEOJSON, true, 4, StorageLevel.MEMORY_ONLY());
        assert spatialRDD.approximateTotalCount == 1001;
        assert spatialRDD.boundaryEnvelope != null;
        assertEquals(spatialRDD.rawSpatialRDD.take(1).get(0).getUserData(), "01\t077\t011501\t5\t1500000US010770115015\t010770115015\t5\tBG\t6844991\t32636");
        assertEquals(spatialRDD.rawSpatialRDD.take(2).get(1).getUserData(), "01\t045\t021102\t4\t1500000US010450211024\t010450211024\t4\tBG\t11360854\t0");
        assertEquals(spatialRDD.fieldNames.toString(), "[STATEFP, COUNTYFP, TRACTCE, BLKGRPCE, AFFGEOID, GEOID, NAME, LSAD, ALAND, AWATER]");
    }

    @Test
    public void testWktConstructor()
    {
        PolygonRDD spatialRDD = new PolygonRDD(sc, InputLocationWkt, FileDataSplitter.WKT, true, StorageLevel.MEMORY_ONLY());
        assert spatialRDD.approximateTotalCount == 103;
        assert spatialRDD.boundaryEnvelope != null;
        assert spatialRDD.rawSpatialRDD.take(1).get(0).getUserData().equals("31\t039\t00835841\t31039\tCuming\tCuming County\t06\tH1\tG4020\t\t\t\tA\t1477895811\t10447360\t+41.9158651\t-096.7885168");
    }

    @Test
    public void testWkbConstructor()
    {
        PolygonRDD spatialRDD = new PolygonRDD(sc, InputLocationWkb, FileDataSplitter.WKB, true, StorageLevel.MEMORY_ONLY());
        assert spatialRDD.approximateTotalCount == 103;
        assert spatialRDD.boundaryEnvelope != null;
        assert spatialRDD.rawSpatialRDD.take(1).get(0).getUserData().equals("31\t039\t00835841\t31039\tCuming\tCuming County\t06\tH1\tG4020\t\t\t\tA\t1477895811\t10447360\t+41.9158651\t-096.7885168");
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
        PolygonRDD spatialRDD = new PolygonRDD(sc, InputLocation, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY());
        spatialRDD.spatialPartitioning(gridType);
        spatialRDD.buildIndex(IndexType.QUADTREE, true);
        if (spatialRDD.indexedRDD.take(1).get(0) instanceof STRtree) {
            List<Polygon> result = spatialRDD.indexedRDD.take(1).get(0).query(spatialRDD.boundaryEnvelope);
        }
        else {
            List<Polygon> result = spatialRDD.indexedRDD.take(1).get(0).query(spatialRDD.boundaryEnvelope);
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

    private String readFirstLine(String filePath)
    {
        BufferedReader br = null;
        FileReader fr = null;
        String cursor = null;
        try {

            //br = new BufferedReader(new FileReader(FILENAME));
            fr = new FileReader(filePath);
            br = new BufferedReader(fr);

            while ((cursor = br.readLine()) != null) {
                break;
            }
        }
        catch (IOException e) {

            e.printStackTrace();
        }
        finally {

            try {

                if (br != null) { br.close(); }

                if (fr != null) { fr.close(); }
            }
            catch (IOException ex) {

                ex.printStackTrace();
            }
        }
        return cursor;
    }
}
