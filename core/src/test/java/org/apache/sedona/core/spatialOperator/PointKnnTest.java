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
package org.apache.sedona.core.spatialOperator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.sedona.common.enums.FileDataSplitter;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.knnJudgement.GeometryDistanceComparator;
import org.apache.sedona.core.spatialRDD.PointRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

// TODO: Auto-generated Javadoc

/**
 * The Class PointKnnTest.
 */
public class PointKnnTest
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
     * The loop times.
     */
    static int loopTimes;

    /**
     * The query point.
     */
    static Point queryPoint;

    /**
     * The top K.
     */
    static int topK;

    /**
     * Once executed before all.
     */
    @BeforeClass
    public static void onceExecutedBeforeAll()
    {
        SparkConf conf = new SparkConf().setAppName("PointKnn").setMaster("local[4]");
        sc = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        prop = new Properties();
        input = PointKnnTest.class.getClassLoader().getResourceAsStream("point.test.properties");

        //Hard code to a file in resource folder. But you can replace it later in the try-catch field in your hdfs system.
        InputLocation = "file://" + PointKnnTest.class.getClassLoader().getResource("primaryroads.csv").getPath();

        offset = 0;
        splitter = null;
        indexType = null;
        numPartitions = 0;
        GeometryFactory fact = new GeometryFactory();
        try {
            // load a properties file
            prop.load(input);
            // There is a field in the property file, you can edit your own file location there.
            // InputLocation = prop.getProperty("inputLocation");
            InputLocation = "file://" + PointKnnTest.class.getClassLoader().getResource(prop.getProperty("inputLocation")).getPath();
            offset = Integer.parseInt(prop.getProperty("offset"));
            splitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
            indexType = IndexType.getIndexType(prop.getProperty("indexType"));
            numPartitions = Integer.parseInt(prop.getProperty("numPartitions"));
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
        queryPoint = fact.createPoint(new Coordinate(-84.01, 34.01));
        topK = 100;
    }

    /**
     * Teardown.
     */
    @AfterClass
    public static void teardown()
    {
        sc.stop();
    }

    /**
     * Test spatial knn query.
     *
     * @throws Exception the exception
     */
    @Test
    public void testSpatialKnnQuery()
            throws Exception
    {
        PointRDD pointRDD = new PointRDD(sc, InputLocation, offset, splitter, false);

        for (int i = 0; i < loopTimes; i++) {
            List<Point> result = KNNQuery.SpatialKnnQuery(pointRDD, queryPoint, topK, false);
            assert result.size() > -1;
            //System.out.println(result.get(0).getUserData().toString());
        }
    }

    /**
     * Test spatial knn query using index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testSpatialKnnQueryUsingIndex()
            throws Exception
    {
        PointRDD pointRDD = new PointRDD(sc, InputLocation, offset, splitter, false);
        pointRDD.buildIndex(IndexType.RTREE, false);
        for (int i = 0; i < loopTimes; i++) {
            List<Point> result = KNNQuery.SpatialKnnQuery(pointRDD, queryPoint, topK, true);
            assert result.size() > -1;
            //System.out.println(result.get(0).getUserData().toString());
        }
    }

    /**
     * Test spatial KNN correctness.
     *
     * @throws Exception the exception
     */
    @Test
    public void testSpatialKNNCorrectness()
            throws Exception
    {
        PointRDD pointRDD = new PointRDD(sc, InputLocation, offset, splitter, false);
        List<Point> resultNoIndex = KNNQuery.SpatialKnnQuery(pointRDD, queryPoint, topK, false);
        pointRDD.buildIndex(IndexType.RTREE, false);
        List<Point> resultWithIndex = KNNQuery.SpatialKnnQuery(pointRDD, queryPoint, topK, true);
        GeometryDistanceComparator geometryDistanceComparator = new GeometryDistanceComparator(queryPoint, true);
        List<Point> mResultNoIndex = new ArrayList<>(resultNoIndex);
        List<Point> mResultWithIndex = new ArrayList<>(resultNoIndex);
        Collections.sort(mResultNoIndex, geometryDistanceComparator);
        Collections.sort(mResultWithIndex, geometryDistanceComparator);
        int difference = 0;
        for (int i = 0; i < topK; i++) {
            if (geometryDistanceComparator.compare(resultNoIndex.get(i), resultWithIndex.get(i)) != 0) {
                difference++;
            }
        }
        assert difference == 0;
    }

    /**
     * Test spatial knn query without useIndex and k is larger than spatialRDD's approximateTotalCount
     *
     * @throws Exception the exception
     */
    @Test
    public void testSpatialKnnQueryTopKlargerThanRowNumber()
            throws Exception
    {
        String inputPath = "file://" + PointKnnTest.class.getClassLoader().getResource("small/onepoint.csv").getPath();
        PointRDD pointRDD = new PointRDD(sc, inputPath, 0, splitter, false);
        //this pointRDD only one row of data
//        System.out.println(pointRDD.getRawSpatialRDD().count());
        for (int i = 0; i < loopTimes; i++) {
            List<Point> result = KNNQuery.SpatialKnnQuery(pointRDD, queryPoint, 5, false);
            assert result.size() == 1;
        }
    }
}