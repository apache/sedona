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
package org.apache.sedona.viz;

import org.apache.sedona.common.enums.FileDataSplitter;
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator;
import org.locationtech.jts.geom.Envelope;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class VizTestBase
{
    protected static SparkConf conf;
    /**
     * The spark context.
     */
    protected static JavaSparkContext sparkContext;

    /**
     * The prop.
     */
    protected static Properties prop;

    /**
     * The input prop.
     */
    protected static InputStream inputProp;

    /**
     * The Point input location.
     */
    protected static String PointInputLocation;

    /**
     * The Point offset.
     */
    protected static Integer PointOffset;

    /**
     * The Point splitter.
     */
    protected static FileDataSplitter PointSplitter;

    /**
     * The Point num partitions.
     */
    protected static Integer PointNumPartitions;

    /**
     * The Rectangle input location.
     */
    protected static String RectangleInputLocation;

    /**
     * The Rectangle offset.
     */
    protected static Integer RectangleOffset;

    /**
     * The Rectangle splitter.
     */
    protected static FileDataSplitter RectangleSplitter;

    /**
     * The Rectangle num partitions.
     */
    protected static Integer RectangleNumPartitions;

    /**
     * The Polygon input location.
     */
    protected static String PolygonInputLocation;

    /**
     * The Polygon offset.
     */
    protected static Integer PolygonOffset;

    /**
     * The Polygon splitter.
     */
    protected static FileDataSplitter PolygonSplitter;

    /**
     * The Polygon num partitions.
     */
    protected static Integer PolygonNumPartitions;

    /**
     * The Line string input location.
     */
    protected static String LineStringInputLocation;

    /**
     * The Line string offset.
     */
    protected static Integer LineStringOffset;

    /**
     * The Line string splitter.
     */
    protected static FileDataSplitter LineStringSplitter;

    /**
     * The Line string num partitions.
     */
    protected static Integer LineStringNumPartitions;

    protected static Envelope USMainLandBoundary;

    /**
     * The US main land boundary.
     */
    protected static void initialize(final String testSuiteName)
            throws Exception
    {
        conf = new SparkConf().setAppName(testSuiteName).setMaster("local[*]");
        conf.set("spark.serializer", KryoSerializer.class.getName());
        conf.set("spark.kryo.registrator", SedonaVizKryoRegistrator.class.getName());

        sparkContext = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        String resourceFolder = System.getProperty("user.dir") + "/../core/src/test/resources/";

        prop = new Properties();

        inputProp = new FileInputStream(resourceFolder + "babylon.point.properties");
        prop.load(inputProp);
        PointInputLocation = resourceFolder + prop.getProperty("inputLocation");
        PointOffset = Integer.parseInt(prop.getProperty("offset"));

        PointSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
        PointNumPartitions = Integer.parseInt(prop.getProperty("numPartitions"));

        inputProp = new FileInputStream(resourceFolder + "babylon.rectangle.properties");
        prop.load(inputProp);
        RectangleInputLocation = resourceFolder + prop.getProperty("inputLocation");
        RectangleOffset = Integer.parseInt(prop.getProperty("offset"));
        RectangleSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
        RectangleNumPartitions = Integer.parseInt(prop.getProperty("numPartitions"));

        inputProp = new FileInputStream(resourceFolder + "babylon.polygon.properties");
        prop.load(inputProp);
        PolygonInputLocation = resourceFolder + prop.getProperty("inputLocation");
        PolygonOffset = Integer.parseInt(prop.getProperty("offset"));
        PolygonSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
        PolygonNumPartitions = Integer.parseInt(prop.getProperty("numPartitions"));

        inputProp = new FileInputStream(resourceFolder + "babylon.linestring.properties");
        prop.load(inputProp);
        LineStringInputLocation = resourceFolder + prop.getProperty("inputLocation");
        LineStringOffset = Integer.parseInt(prop.getProperty("offset"));
        LineStringSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
        LineStringNumPartitions = Integer.parseInt(prop.getProperty("numPartitions"));

        USMainLandBoundary = new Envelope(-126.790180, -64.630926, 24.863836, 50.000);
    }

    /**
     * Sets the up before class.
     *
     * @throws Exception the exception
     */
    @BeforeClass
    public static void setUpBeforeClass()
            throws Exception
    {
        initialize(VizTestBase.class.getSimpleName());
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
        sparkContext.stop();
    }
}
