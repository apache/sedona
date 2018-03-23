/*
 * FILE: GeoSparkVizTestBase
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
package org.datasyslab.geosparkviz;

import com.vividsolutions.jts.geom.Envelope;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator;

import java.io.InputStream;
import java.util.Properties;

public class GeoSparkVizTestBase
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
        conf.set("spark.kryo.registrator", GeoSparkVizKryoRegistrator.class.getName());

        sparkContext = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        prop = new Properties();

        inputProp = GeoSparkVizTestBase.class.getClassLoader().getResourceAsStream("babylon.point.properties");
        prop.load(inputProp);
        PointInputLocation = "file://" + GeoSparkVizTestBase.class.getClassLoader().getResource(prop.getProperty("inputLocation")).getPath();
        PointOffset = Integer.parseInt(prop.getProperty("offset"));
        ;
        PointSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
        PointNumPartitions = Integer.parseInt(prop.getProperty("numPartitions"));

        inputProp = GeoSparkVizTestBase.class.getClassLoader().getResourceAsStream("babylon.rectangle.properties");
        prop.load(inputProp);
        RectangleInputLocation = "file://" + GeoSparkVizTestBase.class.getClassLoader().getResource(prop.getProperty("inputLocation")).getPath();
        RectangleOffset = Integer.parseInt(prop.getProperty("offset"));
        RectangleSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
        RectangleNumPartitions = Integer.parseInt(prop.getProperty("numPartitions"));

        inputProp = GeoSparkVizTestBase.class.getClassLoader().getResourceAsStream("babylon.polygon.properties");
        prop.load(inputProp);
        PolygonInputLocation = "file://" + GeoSparkVizTestBase.class.getClassLoader().getResource(prop.getProperty("inputLocation")).getPath();
        PolygonOffset = Integer.parseInt(prop.getProperty("offset"));
        PolygonSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
        PolygonNumPartitions = Integer.parseInt(prop.getProperty("numPartitions"));

        inputProp = GeoSparkVizTestBase.class.getClassLoader().getResourceAsStream("babylon.linestring.properties");
        prop.load(inputProp);
        LineStringInputLocation = "file://" + GeoSparkVizTestBase.class.getClassLoader().getResource(prop.getProperty("inputLocation")).getPath();
        LineStringOffset = Integer.parseInt(prop.getProperty("offset"));
        LineStringSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
        LineStringNumPartitions = Integer.parseInt(prop.getProperty("numPartitions"));

        USMainLandBoundary = new Envelope(-126.790180, -64.630926, 24.863836, 50.000);
    }
}
