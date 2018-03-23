/*
 * FILE: EarthdataMapperRunnableExample
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
package org.datasyslab.geospark.showcase;

import com.vividsolutions.jts.geom.Envelope;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.formatMapper.EarthdataHDFPointMapper;
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator;
import org.datasyslab.geospark.spatialOperator.RangeQuery;
import org.datasyslab.geospark.spatialRDD.PointRDD;

// TODO: Auto-generated Javadoc

/**
 * The Class EarthdataMapperRunnableExample.
 */
public class EarthdataMapperRunnableExample
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
     * The HDF increment.
     */
    static int HDFIncrement = 5;

    /**
     * The HDF offset.
     */
    static int HDFOffset = 2;

    /**
     * The HDF root group name.
     */
    static String HDFRootGroupName = "MOD_Swath_LST";

    /**
     * The HDF data variable name.
     */
    static String HDFDataVariableName = "LST";

    /**
     * The HDF data variable list.
     */
    static String[] HDFDataVariableList = {"LST", "QC", "Error_LST", "Emis_31", "Emis_32"};

    /**
     * The url prefix.
     */
    static String urlPrefix = "";

    /**
     * The main method.
     *
     * @param args the arguments
     */
    public static void main(String[] args)
    {
        SparkConf conf = new SparkConf().setAppName("EarthdataMapperRunnableExample").setMaster("local[2]");
        conf.set("spark.serializer", KryoSerializer.class.getName());
        conf.set("spark.kryo.registrator", GeoSparkKryoRegistrator.class.getName());
        sc = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        InputLocation = System.getProperty("user.dir") + "/src/test/resources/modis/modis.csv";
        splitter = FileDataSplitter.CSV;
        indexType = IndexType.RTREE;
        queryEnvelope = new Envelope(-90.01, -80.01, 30.01, 40.01);
        numPartitions = 5;
        loopTimes = 1;
        HDFIncrement = 5;
        HDFOffset = 2;
        HDFRootGroupName = "MOD_Swath_LST";
        HDFDataVariableName = "LST";
        urlPrefix = System.getProperty("user.dir") + "/src/test/resources/modis/";
        testSpatialRangeQuery();
        testSpatialRangeQueryUsingIndex();
        sc.stop();
        System.out.println("All GeoSpark Earthdata DEMOs passed!");
    }

    /**
     * Test spatial range query.
     */
    public static void testSpatialRangeQuery()
    {
        EarthdataHDFPointMapper earthdataHDFPoint = new EarthdataHDFPointMapper(HDFIncrement, HDFOffset, HDFRootGroupName,
                HDFDataVariableList, HDFDataVariableName, urlPrefix);
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, numPartitions, earthdataHDFPoint, StorageLevel.MEMORY_ONLY());
        for (int i = 0; i < loopTimes; i++) {
            long resultSize;
            try {
                resultSize = RangeQuery.SpatialRangeQuery(spatialRDD, queryEnvelope, false, false).count();
                assert resultSize > 0;
            }
            catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    /**
     * Test spatial range query using index.
     */
    public static void testSpatialRangeQueryUsingIndex()
    {
        EarthdataHDFPointMapper earthdataHDFPoint = new EarthdataHDFPointMapper(HDFIncrement, HDFOffset, HDFRootGroupName,
                HDFDataVariableList, HDFDataVariableName, urlPrefix);
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, numPartitions, earthdataHDFPoint, StorageLevel.MEMORY_ONLY());
        try {
            spatialRDD.buildIndex(IndexType.RTREE, false);
        }
        catch (Exception e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        for (int i = 0; i < loopTimes; i++) {
            try {
                long resultSize;
                resultSize = RangeQuery.SpatialRangeQuery(spatialRDD, queryEnvelope, false, true).count();
                assert resultSize > 0;
            }
            catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}
