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
package org.apache.sedona.core.utils;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.sedona.common.enums.FileDataSplitter;
import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.knnJudgement.GeometryDistanceComparator;
import org.apache.sedona.core.spatialOperator.JoinQuery;
import org.apache.sedona.core.spatialOperator.KNNQuery;
import org.apache.sedona.core.spatialOperator.RangeQuery;
import org.apache.sedona.core.spatialRDD.PointRDD;
import org.apache.sedona.core.spatialRDD.PolygonRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import scala.Tuple2;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

// TODO: Auto-generated Javadoc

/**
 * The Class CRSTransformationTest.
 */
public class CRSTransformationTest
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
     * The query point.
     */
    static Point queryPoint;

    /**
     * The grid type.
     */
    static GridType gridType;

    /**
     * The Input location query polygon.
     */
    static String InputLocationQueryPolygon;

    /**
     * The top K.
     */
    static int topK;

    /**
     * Sets the up before class.
     *
     * @throws Exception the exception
     */
    @BeforeClass
    public static void setUpBeforeClass()
            throws Exception
    {
        SparkConf conf = new SparkConf().setAppName("PointRange").setMaster("local[2]");
        sc = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        prop = new Properties();
        input = CRSTransformationTest.class.getClassLoader().getResourceAsStream("crs.test.properties");

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
            InputLocation = "file://" + CRSTransformationTest.class.getClassLoader().getResource(prop.getProperty("inputLocation")).getPath();
            InputLocationQueryPolygon = "file://" + CRSTransformationTest.class.getClassLoader().getResource(prop.getProperty("queryPolygonSet")).getPath();
            offset = Integer.parseInt(prop.getProperty("offset"));
            splitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
            gridType = GridType.getGridType(prop.getProperty("gridType"));
            indexType = IndexType.getIndexType(prop.getProperty("indexType"));
            numPartitions = Integer.parseInt(prop.getProperty("numPartitions"));
            queryEnvelope = new Envelope(30.01, 40.01, -90.01, -80.01);
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

        queryPoint = fact.createPoint(new Coordinate(34.01, -84.01));
        topK = 100;
    }

    /**
     * Tear down.
     *
     * @throws Exception the exception
     */
    @AfterClass
    public static void tearDown()
            throws Exception
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
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, StorageLevel.MEMORY_ONLY(), "epsg:4326", "epsg:3005");
        for (int i = 0; i < loopTimes; i++) {
            long resultSize = RangeQuery.SpatialRangeQuery(spatialRDD, queryEnvelope, false, false).count();
            assert resultSize == 3127;
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
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, StorageLevel.MEMORY_ONLY(), "epsg:4326", "epsg:3005");
        spatialRDD.buildIndex(IndexType.RTREE, false);
        for (int i = 0; i < loopTimes; i++) {
            long resultSize = RangeQuery.SpatialRangeQuery(spatialRDD, queryEnvelope, false, true).count();
            assert resultSize == 3127;
        }
        assert RangeQuery.SpatialRangeQuery(spatialRDD, queryEnvelope, false, true).take(10).get(1).getUserData().toString() != null;
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
        PointRDD pointRDD = new PointRDD(sc, InputLocation, offset, splitter, true, StorageLevel.MEMORY_ONLY(), "epsg:4326", "epsg:3005");

        for (int i = 0; i < loopTimes; i++) {
            List<Point> result = KNNQuery.SpatialKnnQuery(pointRDD, queryPoint, topK, false);
            assert result.size() > 0;
            assert result.get(0).getUserData().toString() != null;
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
        PointRDD pointRDD = new PointRDD(sc, InputLocation, offset, splitter, true, StorageLevel.MEMORY_ONLY(), "epsg:4326", "epsg:3005");
        pointRDD.buildIndex(IndexType.RTREE, false);
        for (int i = 0; i < loopTimes; i++) {
            List<Point> result = KNNQuery.SpatialKnnQuery(pointRDD, queryPoint, topK, true);
            assert result.size() > 0;
            assert result.get(0).getUserData().toString() != null;
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
        PointRDD pointRDD = new PointRDD(sc, InputLocation, offset, splitter, true, StorageLevel.MEMORY_ONLY(), "epsg:4326", "epsg:3005");
        List<Point> resultNoIndex = KNNQuery.SpatialKnnQuery(pointRDD, queryPoint, topK, false);
        pointRDD.buildIndex(IndexType.RTREE, false);
        List<Point> resultWithIndex = KNNQuery.SpatialKnnQuery(pointRDD, queryPoint, topK, true);
        GeometryDistanceComparator geometryDistanceComparator = new GeometryDistanceComparator(queryPoint, true);
        List<Point> resultNoIndexModifiable = new ArrayList<>(resultNoIndex);
        List<Point> resultWithIndexModifiable = new ArrayList<>(resultWithIndex);
        Collections.sort(resultNoIndexModifiable, geometryDistanceComparator);
        Collections.sort(resultWithIndexModifiable, geometryDistanceComparator);
        int difference = 0;
        for (int i = 0; i < topK; i++) {
            if (geometryDistanceComparator.compare(resultNoIndex.get(i), resultWithIndex.get(i)) != 0) {
                difference++;
            }
        }
        assert difference == 0;
    }

    /**
     * Test spatial join query with polygon RDD.
     *
     * @throws Exception the exception
     */
    @Test
    public void testSpatialJoinQueryWithPolygonRDD()
            throws Exception
    {

        PolygonRDD queryRDD = new PolygonRDD(sc, InputLocationQueryPolygon, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY(), "epsg:4326", "epsg:3005");

        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY(), "epsg:4326", "epsg:3005");

        spatialRDD.spatialPartitioning(gridType);

        queryRDD.spatialPartitioning(spatialRDD.getPartitioner());

        List<Tuple2<Polygon, List<Point>>> result = JoinQuery.SpatialJoinQuery(spatialRDD, queryRDD, false, true).collect();

        assert result.get(1)._1().getUserData() != null;
        for (int i = 0; i < result.size(); i++) {
            assert result.get(i)._2().size() == 0 || result.get(i)._2().iterator().next().getUserData() != null;
        }
    }

    /**
     * Test spatial join query with polygon RDD using R tree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testSpatialJoinQueryWithPolygonRDDUsingRTreeIndex()
            throws Exception
    {

        PolygonRDD queryRDD = new PolygonRDD(sc, InputLocationQueryPolygon, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY(), "epsg:4326", "epsg:3005");

        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, numPartitions, StorageLevel.MEMORY_ONLY(), "epsg:4326", "epsg:3005");

        spatialRDD.spatialPartitioning(gridType);

        spatialRDD.buildIndex(IndexType.RTREE, true);

        queryRDD.spatialPartitioning(spatialRDD.getPartitioner());

        List<Tuple2<Polygon, List<Point>>> result = JoinQuery.SpatialJoinQuery(spatialRDD, queryRDD, false, true).collect();

        assert result.get(1)._1().getUserData() != null;
        for (int i = 0; i < result.size(); i++) {
            assert result.get(i)._2().size() == 0 || result.get(i)._2().iterator().next().getUserData() != null;
        }
    }
}
