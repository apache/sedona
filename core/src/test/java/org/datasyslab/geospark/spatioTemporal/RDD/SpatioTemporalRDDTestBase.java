/*
 * FILE: SpatioTemporalRDDTestBase
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
package org.datasyslab.geospark.spatioTemporal.RDD;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.datasyslab.geospark.GeoSparkTestBase;
import org.datasyslab.geospark.SpatioTemporalObjects.Cube;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.enums.SpatioTemporalGridType;

/**
 * The Class SpatioTemporalRDDTestBase.
 */
public class SpatioTemporalRDDTestBase
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
    protected static Cube inputBoundary;

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
    static SpatioTemporalGridType gridType;

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
        ClassLoader classLoader = SpatioTemporalRDDTestBase.class.getClassLoader();
        input = classLoader.getResourceAsStream(propertiesFileName);

        offset = 0;
        splitter = null;
        gridType = null;
        numPartitions = 0;

        try {
            // load a properties file
            prop.load(input);
            // There is a field in the property file, you can edit your own file location there.
            // InputLocation = prop.getProperty("inputLocation");
            InputLocation = "file://" + classLoader.getResource(prop.getProperty("inputLocation")).getPath();
            inputCount = Long.parseLong(prop.getProperty("inputCount"));
            String[] coordinates = prop.getProperty("inputBoundary").split(",");
            inputBoundary = new Cube(
                    Double.parseDouble(coordinates[0]),
                    Double.parseDouble(coordinates[1]),
                    Double.parseDouble(coordinates[2]),
                    Double.parseDouble(coordinates[3]),
                    Double.parseDouble(coordinates[4]),
                    Double.parseDouble(coordinates[5]));
            offset = Integer.parseInt(prop.getProperty("offset"));
            splitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
            gridType = SpatioTemporalGridType.getGridType(prop.getProperty("gridType"));
            //indexType = IndexType.getIndexType(prop.getProperty("indexType"));
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
