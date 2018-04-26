/*
 * FILE: PolygonRangeTest
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
package org.datasyslab.geospark.spatialOperator;

import com.vividsolutions.jts.geom.Envelope;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

// TODO: Auto-generated Javadoc

/**
 * The Class PolygonRangeTest.
 */
public class PolygonRangeTest
{

    /**
     * The sc.
     */
    public static JavaSparkContext sc;

    /**
     * The prop.
     */
    static Properties prop;

    /**
     * The input.
     */
    static InputStream input;

    /**
     * The Input location.
     */
    static String InputLocation;

    /**
     * The offset.
     */
    static Integer offset;

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
     * Once executed before all.
     */
    @BeforeClass
    public static void onceExecutedBeforeAll()
    {
        SparkConf conf = new SparkConf().setAppName("PolygonRange").setMaster("local[2]");
        sc = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        prop = new Properties();
        input = PolygonRangeTest.class.getClassLoader().getResourceAsStream("polygon.test.properties");

        //Hard code to a file in resource folder. But you can replace it later in the try-catch field in your hdfs system.
        InputLocation = "file://" + PolygonRangeTest.class.getClassLoader().getResource("primaryroads-polygon.csv").getPath();

        offset = 0;
        splitter = null;
        indexType = null;
        numPartitions = 0;

        try {
            // load a properties file
            prop.load(input);
            // There is a field in the property file, you can edit your own file location there.
            // InputLocation = prop.getProperty("inputLocation");
            InputLocation = "file://" + PolygonRangeTest.class.getClassLoader().getResource(prop.getProperty("inputLocation")).getPath();
            offset = Integer.parseInt(prop.getProperty("offset"));
            splitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
            indexType = IndexType.getIndexType(prop.getProperty("indexType"));
            numPartitions = Integer.parseInt(prop.getProperty("numPartitions"));
            queryEnvelope = new Envelope(-85.01, -60.01, 34.01, 50.01);
            loopTimes = 5;
        }
        catch (IOException ex) {
            ex.printStackTrace();
        }
        finally {
            if (input != null) {
                try {
                    input.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
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
        PolygonRDD spatialRDD = new PolygonRDD(sc, InputLocation, splitter, true, StorageLevel.MEMORY_ONLY());
        for (int i = 0; i < loopTimes; i++) {
            long resultSize = RangeQuery.SpatialRangeQuery(spatialRDD, queryEnvelope, false, false).count();
            assertEquals(resultSize, 704);
        }
        assert RangeQuery.SpatialRangeQuery(spatialRDD, queryEnvelope, false, false).take(10).get(1).getUserData().toString() != null;
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
        PolygonRDD spatialRDD = new PolygonRDD(sc, InputLocation, splitter, true, StorageLevel.MEMORY_ONLY());
        spatialRDD.buildIndex(IndexType.RTREE, false);
        for (int i = 0; i < loopTimes; i++) {
            long resultSize = RangeQuery.SpatialRangeQuery(spatialRDD, queryEnvelope, false, true).count();
            assertEquals(resultSize, 704);
        }
        assert RangeQuery.SpatialRangeQuery(spatialRDD, queryEnvelope, false, true).take(10).get(1).getUserData().toString() != null;
    }
}