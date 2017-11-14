/**
 * FILE: JoinTestBase.java
 * PATH: org.datasyslab.geospark.spatialOperator.JoinTestBase.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialOperator;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.GeoSparkTestBase;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.spatialRDD.LineStringRDD;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;
import scala.Tuple2;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

class JoinTestBase extends GeoSparkTestBase {

    /** The prop. */
    static Properties prop;

    /** The Input location. */
    static String InputLocation;

    /** The Input location query window. */
    static String InputLocationQueryWindow;

    /** The Input location query polygon. */
    static String InputLocationQueryPolygon;

    /** The offset. */
    static Integer offset;

    /** The splitter. */
    static FileDataSplitter splitter;

    /** The index type. */
    static IndexType indexType;

    /** The num partitions. */
    static Integer numPartitions;

    protected final GridType gridType;

    protected final boolean useLegacyPartitionAPIs;

    protected JoinTestBase(GridType gridType, boolean useLegacyPartitionAPIs, int numPartitions) {
        this.gridType = gridType;
        this.useLegacyPartitionAPIs = useLegacyPartitionAPIs;
        this.numPartitions = numPartitions;
    }

    protected static void initialize(final String testSuiteName, final String propertiesFileName) {
        GeoSparkTestBase.initialize(testSuiteName);

        prop = new Properties();
        final ClassLoader classLoader = JoinTestBase.class.getClassLoader();
        final InputStream input = classLoader.getResourceAsStream(propertiesFileName);
        offset = 0;
        splitter = null;
        indexType = null;

        try {
            // load a properties file
            prop.load(input);
            InputLocation = "file://"+ classLoader.getResource(prop.getProperty("inputLocation")).getPath();
            InputLocationQueryWindow = "file://"+ classLoader.getResource(prop.getProperty("queryWindowSet")).getPath();
            InputLocationQueryPolygon = "file://"+ classLoader.getResource(prop.getProperty("queryPolygonSet")).getPath();
            offset = Integer.parseInt(prop.getProperty("offset"));
            splitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
            indexType = IndexType.getIndexType(prop.getProperty("indexType"));
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    protected PointRDD createPointRDD(String location) {
        final PointRDD rdd = new PointRDD(sc, location, splitter, true, numPartitions);
        return new PointRDD(rdd.rawSpatialRDD, StorageLevel.MEMORY_ONLY());
    }

    protected LineStringRDD createLineStringRDD(String location) {
        final LineStringRDD rdd = new LineStringRDD(sc, location, splitter, true, numPartitions);
        return new LineStringRDD(rdd.rawSpatialRDD, StorageLevel.MEMORY_ONLY());
    }

    protected PolygonRDD createPolygonRDD(String location) {
        final PolygonRDD rdd = new PolygonRDD(sc, location, splitter, true, numPartitions);
        return new PolygonRDD(rdd.rawSpatialRDD, StorageLevel.MEMORY_ONLY());
    }

    protected RectangleRDD createRectangleRDD(String location) {
        final RectangleRDD rdd = new RectangleRDD(sc, location, splitter, true, numPartitions);
        return new RectangleRDD(rdd.rawSpatialRDD, StorageLevel.MEMORY_ONLY());
    }

    protected void partitionRdds(SpatialRDD<? extends Geometry> queryRDD,
                                 SpatialRDD<? extends Geometry> spatialRDD) throws Exception {
        spatialRDD.spatialPartitioning(gridType);
        if (useLegacyPartitionAPIs) {
            if (gridType != GridType.QUADTREE) {
                queryRDD.spatialPartitioning(spatialRDD.grids);
            } else {
                queryRDD.spatialPartitioning(spatialRDD.partitionTree);
            }
        } else {
            queryRDD.spatialPartitioning(spatialRDD.getPartitioner());
        }
    }

    protected boolean expectToPreserveOriginalDuplicates() {
        return gridType == GridType.QUADTREE || gridType == GridType.KDBTREE;
    }

    protected <T extends Geometry> long countJoinResults(List<Tuple2<Polygon, HashSet<T>>> results) {
        int count = 0;
        for (final Tuple2<Polygon, HashSet<T>> tuple : results) {
            count += tuple._2().size();
        }
        return count;
    }

    protected  <T extends Geometry> void sanityCheckJoinResults(List<Tuple2<Polygon, HashSet<T>>> results) {
        for (final Tuple2<Polygon, HashSet<T>> tuple : results) {
            assertNotNull(tuple._1().getUserData());
            assertFalse(tuple._2().isEmpty());
            for (final T shape : tuple._2()) {
                assertNotNull(shape.getUserData());
                assertTrue(tuple._1().intersects(shape));
            }
        }
    }

    protected  <T extends Geometry> void sanityCheckFlatJoinResults(List<Tuple2<Polygon, T>> results) {
        for (final Tuple2<Polygon, T> tuple : results) {
            assertNotNull(tuple._1().getUserData());
            assertNotNull(tuple._2().getUserData());
            assertTrue(tuple._1().intersects(tuple._2()));
        }
    }
}
