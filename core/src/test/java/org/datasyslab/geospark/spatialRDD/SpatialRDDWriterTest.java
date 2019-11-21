package org.datasyslab.geospark.spatialRDD;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.NullArgumentException;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class SpatialRDDWriterTest
        extends SpatialRDDTestBase{

    private static String testSaveAsWKBWithData;
    private static String testSaveAsWKB;
    private static String testSaveAsEmptyWKB;
    
    /**
     * Once executed before all.
     */
    @BeforeClass
    public static void onceExecutedBeforeAll()
    {
        initialize(SpatialRDDWriterTest.class.getSimpleName(), "point.test.properties");
        String wkbFolder = System.getProperty("user.dir") + "/target/test-classes/wkb/";

        testSaveAsWKBWithData = wkbFolder + "saveAsWKBWithData";
        testSaveAsWKB = wkbFolder + "testSaveAsWKB";
        testSaveAsEmptyWKB = wkbFolder + "testSaveAsEmptyWKB";
    }

    /**
     * Test save as wkb with data 
     *
     */
    @Test
    public void testSaveAsWKBWithData() throws IOException {
        File wkb = new File(testSaveAsWKBWithData);
        if (wkb.exists()){ FileUtils.deleteDirectory(wkb);}
        
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY());
        spatialRDD.saveAsWKB(testSaveAsWKBWithData);
        
        // Load the saved rdd and compare them
        PointRDD resultWKB = new PointRDD(sc, testSaveAsWKBWithData, 0, FileDataSplitter.WKB, true, numPartitions, StorageLevel.MEMORY_ONLY());
        
        assertEquals(resultWKB.rawSpatialRDD.count(), spatialRDD.rawSpatialRDD.count());
        assertEquals(resultWKB.rawSpatialRDD.takeOrdered(5), spatialRDD.rawSpatialRDD.takeOrdered(5));
    }

    /**
     * Test save as wkb.
     *
     */
    @Test
    public void testSaveAsWKB() throws IOException {
        File wkb = new File(testSaveAsWKB);
        if (wkb.exists()){ FileUtils.deleteDirectory(wkb);}

        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, false, numPartitions, StorageLevel.MEMORY_ONLY());
        spatialRDD.saveAsWKB(testSaveAsWKB);

        // Load the saved rdd and compare them
        PointRDD resultWKB = new PointRDD(sc, testSaveAsWKB, 0, FileDataSplitter.WKB, false, numPartitions, StorageLevel.MEMORY_ONLY());

        assertEquals(resultWKB.rawSpatialRDD.count(), spatialRDD.rawSpatialRDD.count());
        assertEquals(resultWKB.rawSpatialRDD.takeOrdered(5), spatialRDD.rawSpatialRDD.takeOrdered(5));
    }

    /**
     * Test throws NullArgumentException when Spatial RDD is null.
     *
     */
    @Test(expected = NullArgumentException.class)
    public void testSaveAsEmptyWKB() {

        PointRDD emptySpatialRDD = new PointRDD();
        emptySpatialRDD.saveAsWKB(testSaveAsEmptyWKB);
    }
    
    /**
     * Tear down.
     */
    @AfterClass
    public static void TearDown()
    {
        sc.stop();
    }
}
