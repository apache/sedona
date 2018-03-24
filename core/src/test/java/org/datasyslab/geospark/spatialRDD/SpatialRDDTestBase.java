/**
 * FILE: SpatialRDDTestBase.java
 * PATH: org.datasyslab.geospark.spatialRDD.SpatialRDDTestBase.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialRDD;

import com.vividsolutions.jts.geom.Envelope;
import org.datasyslab.geospark.GeoSparkTestBase;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class SpatialRDDTestBase
        extends GeoSparkTestBase
{
    /**
     * The prop.
     */
    static Properties prop;

    /**
     * The input.
     */
    static InputStream input;

    protected static long inputCount;
    protected static Envelope inputBoundary;

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
     * The grid type.
     */
    static GridType gridType;

    /**
     * The index type.
     */
    static IndexType indexType;

    /**
     * The num partitions.
     */
    static Integer numPartitions;

    /**
     * Once executed before all.
     */
    protected static void initialize(final String testSuiteName, final String propertiesFileName)
    {
        GeoSparkTestBase.initialize(testSuiteName);

        prop = new Properties();
        ClassLoader classLoader = SpatialRDDTestBase.class.getClassLoader();
        input = classLoader.getResourceAsStream(propertiesFileName);

        offset = 0;
        splitter = null;
        gridType = null;
        indexType = null;
        numPartitions = 0;

        try {
            // load a properties file
            prop.load(input);
            // There is a field in the property file, you can edit your own file location there.
            // InputLocation = prop.getProperty("inputLocation");
            InputLocation = "file://" + classLoader.getResource(prop.getProperty("inputLocation")).getPath();
            inputCount = Long.parseLong(prop.getProperty("inputCount"));
            String[] coordinates = prop.getProperty("inputBoundary").split(",");
            inputBoundary = new Envelope(
                    Double.parseDouble(coordinates[0]),
                    Double.parseDouble(coordinates[1]),
                    Double.parseDouble(coordinates[2]),
                    Double.parseDouble(coordinates[3]));
            offset = Integer.parseInt(prop.getProperty("offset"));
            splitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
            gridType = GridType.getGridType(prop.getProperty("gridType"));
            indexType = IndexType.getIndexType(prop.getProperty("indexType"));
            numPartitions = Integer.parseInt(prop.getProperty("numPartitions"));
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
}
