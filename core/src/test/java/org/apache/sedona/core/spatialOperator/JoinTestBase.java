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

import org.apache.sedona.core.TestBase;
import org.apache.sedona.common.enums.FileDataSplitter;
import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.spatialRDD.LineStringRDD;
import org.apache.sedona.core.spatialRDD.PointRDD;
import org.apache.sedona.core.spatialRDD.PolygonRDD;
import org.apache.sedona.core.spatialRDD.RectangleRDD;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.spark.storage.StorageLevel;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;
import scala.Tuple2;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

class JoinTestBase
        extends TestBase
{

    /**
     * The prop.
     */
    static Properties prop;

    /**
     * The Input location.
     */
    static String InputLocation;

    /**
     * The Input location query window.
     */
    static String InputLocationQueryWindow;

    /**
     * The Input location query polygon.
     */
    static String InputLocationQueryPolygon;

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

    protected final GridType gridType;

    protected JoinTestBase(GridType gridType, int numPartitions)
    {
        this.gridType = gridType;
        JoinTestBase.numPartitions = numPartitions;
    }

    protected static void initialize(final String testSuiteName, final String propertiesFileName)
    {
        TestBase.initialize(testSuiteName);

        prop = new Properties();
        final ClassLoader classLoader = JoinTestBase.class.getClassLoader();
        final InputStream input = classLoader.getResourceAsStream(propertiesFileName);
        offset = 0;
        splitter = null;
        indexType = null;

        try {
            // load a properties file
            prop.load(input);
            InputLocation = "file://" + classLoader.getResource(prop.getProperty("inputLocation")).getPath();
            InputLocationQueryWindow = "file://" + classLoader.getResource(prop.getProperty("queryWindowSet")).getPath();
            InputLocationQueryPolygon = "file://" + classLoader.getResource(prop.getProperty("queryPolygonSet")).getPath();
            offset = Integer.parseInt(prop.getProperty("offset"));
            splitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
            indexType = IndexType.getIndexType(prop.getProperty("indexType"));
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

    protected PointRDD createPointRDD(String location)
    {
        final PointRDD rdd = new PointRDD(sc, location, 1, splitter, false, numPartitions);
        return new PointRDD(rdd.rawSpatialRDD, StorageLevel.MEMORY_ONLY());
    }

    protected LineStringRDD createLineStringRDD(String location)
    {
        final LineStringRDD rdd = new LineStringRDD(sc, location, splitter, true, numPartitions);
        return new LineStringRDD(rdd.rawSpatialRDD, StorageLevel.MEMORY_ONLY());
    }

    protected PolygonRDD createPolygonRDD(String location)
    {
        final PolygonRDD rdd = new PolygonRDD(sc, location, splitter, true, numPartitions);
        return new PolygonRDD(rdd.rawSpatialRDD, StorageLevel.MEMORY_ONLY());
    }

    protected RectangleRDD createRectangleRDD(String location)
    {
        final RectangleRDD rdd = new RectangleRDD(sc, location, splitter, true, numPartitions);
        return new RectangleRDD(rdd.rawSpatialRDD, StorageLevel.MEMORY_ONLY());
    }

    protected void partitionRdds(SpatialRDD<? extends Geometry> queryRDD,
            SpatialRDD<? extends Geometry> spatialRDD)
            throws Exception
    {
        spatialRDD.spatialPartitioning(gridType);
        queryRDD.spatialPartitioning(spatialRDD.getPartitioner());
    }

    protected boolean expectToPreserveOriginalDuplicates()
    {
        return gridType == GridType.QUADTREE || gridType == GridType.KDBTREE;
    }

    protected <T extends Geometry> long countJoinResults(List<Tuple2<Polygon, List<T>>> results)
    {
        int count = 0;
        for (final Tuple2<Polygon, List<T>> tuple : results) {
            count += tuple._2().size();
        }
        return count;
    }

    protected <T extends Geometry> void sanityCheckJoinResults(List<Tuple2<Polygon, List<T>>> results)
    {
        for (final Tuple2<Polygon, List<T>> tuple : results) {
            assertFalse(tuple._2().isEmpty());
            for (final T shape : tuple._2()) {
                assertTrue(tuple._1().intersects(shape));
            }
        }
    }

    protected <T extends Geometry> void sanityCheckFlatJoinResults(List<Tuple2<Polygon, T>> results)
    {
        for (final Tuple2<Polygon, T> tuple : results) {
            assertTrue(tuple._1().intersects(tuple._2()));
        }
    }
}
