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

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.NullArgumentException;
import org.apache.sedona.common.utils.GeomUtils;
import org.apache.sedona.common.enums.FileDataSplitter;
import org.apache.spark.storage.StorageLevel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.Point;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SpatialRDDWriterTest
        extends SpatialRDDTestBase
{

    private static String testSaveAsWKBWithData;
    private static String testSaveAsWKB;
    private static String testSaveAsEmptyWKB;
    private static String testSaveAsWKT;
    private static String testSaveAsWKTWithData;

    /**
     * Once executed before all.
     */
    @BeforeClass
    public static void onceExecutedBeforeAll()
    {
        initialize(SpatialRDDWriterTest.class.getSimpleName(), "point.test.properties");
        String wkbFolder = System.getProperty("user.dir") + "/target/test-classes/wkb/";
        String wktFolder = System.getProperty("user.dir") + "/target/test-classes/wkt/";

        testSaveAsWKBWithData = wkbFolder + "testSaveAsWKBWithData";
        testSaveAsWKB = wkbFolder + "testSaveAsWKB";
        testSaveAsEmptyWKB = wkbFolder + "testSaveAsEmptyWKB";
        testSaveAsWKT = wktFolder + "testSaveAsWKT";
        testSaveAsWKTWithData = wktFolder + "testSaveAsWKTWithData";
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
     * Test save as wkb with data
     */
    @Test
    public void testSaveAsWKBWithData()
            throws IOException
    {
        File wkb = new File(testSaveAsWKBWithData);
        if (wkb.exists()) { FileUtils.deleteDirectory(wkb);}

        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY());
        spatialRDD.saveAsWKB(testSaveAsWKBWithData);

        // Load the saved rdd and compare them
        PointRDD resultWKB = new PointRDD(sc, testSaveAsWKBWithData, 0, FileDataSplitter.WKB, true, numPartitions, StorageLevel.MEMORY_ONLY());

        assertEquals(resultWKB.rawSpatialRDD.count(), spatialRDD.rawSpatialRDD.count());
        verifyResult(resultWKB.rawSpatialRDD.takeOrdered(5), spatialRDD.rawSpatialRDD.takeOrdered(5));
    }

    /**
     * Test save as wkt with data
     */
    @Test
    public void testSaveAsWKTWithData()
            throws IOException
    {
        File wkt = new File(testSaveAsWKTWithData);
        if (wkt.exists()) {FileUtils.deleteDirectory(wkt);}

        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY());
        spatialRDD.saveAsWKT(testSaveAsWKTWithData);

        // Load the saved rdd and compare them
        PointRDD resultWKT = new PointRDD(sc, testSaveAsWKTWithData, 0, FileDataSplitter.WKT, true, numPartitions, StorageLevel.MEMORY_ONLY());

        assertEquals(resultWKT.rawSpatialRDD.count(), spatialRDD.rawSpatialRDD.count());
        verifyResult(resultWKT.rawSpatialRDD.takeOrdered(5), spatialRDD.rawSpatialRDD.takeOrdered(5));
    }

    /**
     * Test save as wkb.
     */
    @Test
    public void testSaveAsWKB()
            throws IOException
    {
        File wkb = new File(testSaveAsWKB);
        if (wkb.exists()) { FileUtils.deleteDirectory(wkb);}

        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, false, numPartitions, StorageLevel.MEMORY_ONLY());
        spatialRDD.saveAsWKB(testSaveAsWKB);

        // Load the saved rdd and compare them
        PointRDD resultWKB = new PointRDD(sc, testSaveAsWKB, 0, FileDataSplitter.WKB, false, numPartitions, StorageLevel.MEMORY_ONLY());

        assertEquals(resultWKB.rawSpatialRDD.count(), spatialRDD.rawSpatialRDD.count());
        verifyResult(resultWKB.rawSpatialRDD.takeOrdered(5), spatialRDD.rawSpatialRDD.takeOrdered(5));
    }

    /**
     * Test save as wkt.
     */
    @Test
    public void testSaveAsWKT()
            throws IOException
    {
        File wkt = new File(testSaveAsWKT);
        if (wkt.exists()) {FileUtils.deleteDirectory(wkt);}

        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, false, numPartitions, StorageLevel.MEMORY_ONLY());
        spatialRDD.saveAsWKT(testSaveAsWKT);

        // Load the saved rdd and compare them
        PointRDD resultWKT = new PointRDD(sc, testSaveAsWKT, 0, FileDataSplitter.WKT, false, numPartitions, StorageLevel.MEMORY_ONLY());

        assertEquals(resultWKT.rawSpatialRDD.count(), spatialRDD.rawSpatialRDD.count());
        verifyResult(resultWKT.rawSpatialRDD.takeOrdered(5), spatialRDD.rawSpatialRDD.takeOrdered(5));
    }

    /**
     * Test throws NullArgumentException when Spatial RDD is null.
     */
    @Test(expected = NullArgumentException.class)
    public void testSaveAsEmptyWKB()
    {

        PointRDD emptySpatialRDD = new PointRDD();
        emptySpatialRDD.saveAsWKB(testSaveAsEmptyWKB);
    }

    private void verifyResult(List<Point> left, List<Point> right){
        assertEquals(left.size(), right.size());
        for (int i = 0; i < left.size(); i++) {
            assertTrue(GeomUtils.equalsExactGeom(left.get(i), right.get(i)));
        }
    }
}
