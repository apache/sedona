/**
 * FILE: EarthdataHDFTest.java
 * PATH: org.datasyslab.geospark.io.EarthdataHDFTest.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.io;

import com.vividsolutions.jts.geom.Envelope;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.formatMapper.EarthdataHDFPointMapper;
import org.datasyslab.geospark.spatialOperator.RangeQuery;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

// TODO: Auto-generated Javadoc

/**
 * The Class EarthdataHDFTest.
 */
public class EarthdataHDFTest
{

    /**
     * The sc.
     */
    public static JavaSparkContext sc;

    /**
     * The Input location.
     */
    static String InputLocation;

    /**
     * The splitter.
     */
    static FileDataSplitter splitter;

    /**
     * The index type.
     */
    static IndexType indexType;

    /**
     * The num partitions.
     */
    static Integer numPartitions;

    /**
     * The query envelope.
     */
    static Envelope queryEnvelope;

    /**
     * The loop times.
     */
    static int loopTimes;

    /**
     * The HD fincrement.
     */
    static int HDFincrement = 5;

    /**
     * The HD foffset.
     */
    static int HDFoffset = 2;

    /**
     * The HD froot group name.
     */
    static String HDFrootGroupName = "MOD_Swath_LST";

    /**
     * The HDF data variable list.
     */
    static String[] HDFDataVariableList = {"LST", "QC", "Error_LST", "Emis_31", "Emis_32"};

    /**
     * The HDF data variable name.
     */
    static String HDFDataVariableName = "LST";

    /**
     * The url prefix.
     */
    static String urlPrefix = "";

    /**
     * Once executed before all.
     */
    @BeforeClass
    public static void onceExecutedBeforeAll()
    {
        SparkConf conf = new SparkConf().setAppName("EarthdataHDFTest").setMaster("local[2]");
        sc = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        InputLocation = "file://" + EarthdataHDFTest.class.getClassLoader().getResource("modis/modis.csv").getPath();
        splitter = FileDataSplitter.CSV;
        indexType = IndexType.RTREE;
        queryEnvelope = new Envelope(-90.01, -80.01, 30.01, 40.01);
        numPartitions = 5;
        loopTimes = 1;
        HDFincrement = 5;
        HDFoffset = 2;
        HDFrootGroupName = "MOD_Swath_LST";
        HDFDataVariableName = "LST";
        urlPrefix = System.getProperty("user.dir") + "/src/test/resources/modis/";
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
     * Test spatial range query.
     *
     * @throws Exception the exception
     */
    @Test
    public void testSpatialRangeQuery()
            throws Exception
    {
        EarthdataHDFPointMapper earthdataHDFPoint = new EarthdataHDFPointMapper(HDFincrement, HDFoffset, HDFrootGroupName,
                HDFDataVariableList, HDFDataVariableName, urlPrefix);
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, numPartitions, earthdataHDFPoint, StorageLevel.MEMORY_ONLY());
        for (int i = 0; i < loopTimes; i++) {
            long resultSize = RangeQuery.SpatialRangeQuery(spatialRDD, queryEnvelope, false, false).count();
            assert resultSize > -1;
        }
    }

    /**
     * Test spatial range query using index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testSpatialRangeQueryUsingIndex()
            throws Exception
    {
        EarthdataHDFPointMapper earthdataHDFPoint = new EarthdataHDFPointMapper(HDFincrement, HDFoffset, HDFrootGroupName,
                HDFDataVariableList, HDFDataVariableName, urlPrefix);
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, numPartitions, earthdataHDFPoint, StorageLevel.MEMORY_ONLY());
        spatialRDD.buildIndex(IndexType.RTREE, false);
        for (int i = 0; i < loopTimes; i++) {
            long resultSize = RangeQuery.SpatialRangeQuery(spatialRDD, queryEnvelope, false, true).count();
            assert resultSize > -1;
        }
    }
}