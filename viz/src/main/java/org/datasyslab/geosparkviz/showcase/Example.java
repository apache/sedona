/*
 * FILE: Example
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
package org.datasyslab.geosparkviz.showcase;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Polygon;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.formatMapper.EarthdataHDFPointMapper;
import org.datasyslab.geospark.spatialOperator.JoinQuery;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;
import org.datasyslab.geosparkviz.core.ImageGenerator;
import org.datasyslab.geosparkviz.core.ImageStitcher;
import org.datasyslab.geosparkviz.core.RasterOverlayOperator;
import org.datasyslab.geosparkviz.extension.imageGenerator.GeoSparkVizImageGenerator;
import org.datasyslab.geosparkviz.extension.visualizationEffect.ChoroplethMap;
import org.datasyslab.geosparkviz.extension.visualizationEffect.HeatMap;
import org.datasyslab.geosparkviz.extension.visualizationEffect.ScatterPlot;
import org.datasyslab.geosparkviz.utils.ColorizeOption;
import org.datasyslab.geosparkviz.utils.ImageType;

import java.awt.Color;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

// TODO: Auto-generated Javadoc

/**
 * The Class Example.
 */
public class Example
{

    /**
     * The spark context.
     */
    static JavaSparkContext sparkContext;

    /**
     * The prop.
     */
    static Properties prop;

    /**
     * The Point input location.
     */
    static String PointInputLocation;

    /**
     * The Point offset.
     */
    static Integer PointOffset;

    /**
     * The Point splitter.
     */
    static FileDataSplitter PointSplitter;

    /**
     * The Point num partitions.
     */
    static Integer PointNumPartitions;

    /**
     * The Rectangle input location.
     */
    static String RectangleInputLocation;

    /**
     * The Rectangle offset.
     */
    static Integer RectangleOffset;

    /**
     * The Rectangle splitter.
     */
    static FileDataSplitter RectangleSplitter;

    /**
     * The Rectangle num partitions.
     */
    static Integer RectangleNumPartitions;

    /**
     * The Polygon input location.
     */
    static String PolygonInputLocation;

    /**
     * The Polygon offset.
     */
    static Integer PolygonOffset;

    /**
     * The Polygon splitter.
     */
    static FileDataSplitter PolygonSplitter;

    /**
     * The Polygon num partitions.
     */
    static Integer PolygonNumPartitions;

    /**
     * The Line string input location.
     */
    static String LineStringInputLocation;

    /**
     * The Line string offset.
     */
    static Integer LineStringOffset;

    /**
     * The Line string splitter.
     */
    static FileDataSplitter LineStringSplitter;

    /**
     * The Line string num partitions.
     */
    static Integer LineStringNumPartitions;

    /**
     * The US main land boundary.
     */
    static Envelope USMainLandBoundary;

    /**
     * The earthdata input location.
     */
    static String earthdataInputLocation;

    /**
     * The earthdata num partitions.
     */
    static Integer earthdataNumPartitions;

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
     * The HD fswitch XY.
     */
    static boolean HDFswitchXY = true;

    /**
     * The url prefix.
     */
    static String urlPrefix = "";

    /**
     * Builds the scatter plot.
     *
     * @param outputPath the output path
     * @return true, if successful
     */
    public static boolean buildScatterPlot(String outputPath)
    {
        try {
            PolygonRDD spatialRDD = new PolygonRDD(sparkContext, PolygonInputLocation, PolygonSplitter, false, PolygonNumPartitions, StorageLevel.MEMORY_ONLY());
            ScatterPlot visualizationOperator = new ScatterPlot(1000, 600, USMainLandBoundary, false);
            visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.GREEN, true);
            visualizationOperator.Visualize(sparkContext, spatialRDD);
            GeoSparkVizImageGenerator imageGenerator = new GeoSparkVizImageGenerator();
            imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.rasterImage, outputPath, ImageType.PNG);

            visualizationOperator = new ScatterPlot(1000, 600, USMainLandBoundary, false, -1, -1, false, true);
            visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.GREEN, true);
            visualizationOperator.Visualize(sparkContext, spatialRDD);
            imageGenerator = new GeoSparkVizImageGenerator();
            imageGenerator.SaveVectorImageAsLocalFile(visualizationOperator.vectorImage, outputPath, ImageType.SVG);

            visualizationOperator = new ScatterPlot(1000, 600, USMainLandBoundary, false, -1, -1, true, true);
            visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.GREEN, true);
            visualizationOperator.Visualize(sparkContext, spatialRDD);
            imageGenerator = new GeoSparkVizImageGenerator();
            imageGenerator.SaveVectorImageAsLocalFile(visualizationOperator.distributedVectorImage, "file://" + outputPath + "-distributed", ImageType.SVG);
        }
        catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * Builds the heat map.
     *
     * @param outputPath the output path
     * @return true, if successful
     */
    public static boolean buildHeatMap(String outputPath)
    {
        try {
            RectangleRDD spatialRDD = new RectangleRDD(sparkContext, RectangleInputLocation, RectangleSplitter, false, RectangleNumPartitions, StorageLevel.MEMORY_ONLY());
            HeatMap visualizationOperator = new HeatMap(1000, 600, USMainLandBoundary, false, 2);
            visualizationOperator.Visualize(sparkContext, spatialRDD);
            GeoSparkVizImageGenerator imageGenerator = new GeoSparkVizImageGenerator();
            imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.rasterImage, outputPath, ImageType.PNG);
        }
        catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * Builds the choropleth map.
     *
     * @param outputPath the output path
     * @return true, if successful
     */
    public static boolean buildChoroplethMap(String outputPath)
    {
        try {
            PointRDD spatialRDD = new PointRDD(sparkContext, PointInputLocation, PointOffset, PointSplitter, false, PointNumPartitions, StorageLevel.MEMORY_ONLY());
            PolygonRDD queryRDD = new PolygonRDD(sparkContext, PolygonInputLocation, PolygonSplitter, false, PolygonNumPartitions, StorageLevel.MEMORY_ONLY());
            spatialRDD.spatialPartitioning(GridType.RTREE);
            queryRDD.spatialPartitioning(spatialRDD.grids);
            spatialRDD.buildIndex(IndexType.RTREE, true);
            JavaPairRDD<Polygon, Long> joinResult = JoinQuery.SpatialJoinQueryCountByKey(spatialRDD, queryRDD, true, false);

            ChoroplethMap visualizationOperator = new ChoroplethMap(1000, 600, USMainLandBoundary, false);
            visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.RED, true);
            visualizationOperator.Visualize(sparkContext, joinResult);

            ScatterPlot frontImage = new ScatterPlot(1000, 600, USMainLandBoundary, false);
            frontImage.CustomizeColor(0, 0, 0, 255, Color.GREEN, true);
            frontImage.Visualize(sparkContext, queryRDD);

            RasterOverlayOperator overlayOperator = new RasterOverlayOperator(visualizationOperator.rasterImage);
            overlayOperator.JoinImage(frontImage.rasterImage);

            GeoSparkVizImageGenerator imageGenerator = new GeoSparkVizImageGenerator();
            imageGenerator.SaveRasterImageAsLocalFile(overlayOperator.backRasterImage, outputPath, ImageType.PNG);
        }
        catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * Parallel filter render no stitch.
     *
     * @param outputPath the output path
     * @return true, if successful
     */
    public static boolean parallelFilterRenderNoStitch(String outputPath)
    {
        try {
            RectangleRDD spatialRDD = new RectangleRDD(sparkContext, RectangleInputLocation, RectangleSplitter, false, RectangleNumPartitions, StorageLevel.MEMORY_ONLY());
            HeatMap visualizationOperator = new HeatMap(1000, 600, USMainLandBoundary, false, 2, 4, 4, true, true);
            visualizationOperator.Visualize(sparkContext, spatialRDD);
            GeoSparkVizImageGenerator imageGenerator = new GeoSparkVizImageGenerator();
            imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.distributedRasterImage, outputPath, ImageType.PNG);
        }
        catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * Parallel filter render stitch.
     *
     * @param outputPath the output path
     * @return true, if successful
     */
    public static boolean parallelFilterRenderStitch(String outputPath)
    {
        try {
            RectangleRDD spatialRDD = new RectangleRDD(sparkContext, RectangleInputLocation, RectangleSplitter, false, RectangleNumPartitions, StorageLevel.MEMORY_ONLY());
            HeatMap visualizationOperator = new HeatMap(1000, 600, USMainLandBoundary, false, 2, 4, 4, true, true);
            visualizationOperator.Visualize(sparkContext, spatialRDD);
            ImageGenerator imageGenerator = new ImageGenerator();
            imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.distributedRasterImage, outputPath, ImageType.PNG, 0, 4, 4);
            ImageStitcher.stitchImagePartitionsFromLocalFile(outputPath, 1000, 600, 0, 4, 4);
        }
        catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * Earthdata visualization.
     *
     * @param outputPath the output path
     * @return true, if successful
     */
    public static boolean earthdataVisualization(String outputPath)
    {

        try {
            EarthdataHDFPointMapper earthdataHDFPoint = new EarthdataHDFPointMapper(HDFIncrement, HDFOffset, HDFRootGroupName,
                    HDFDataVariableList, HDFDataVariableName, HDFswitchXY, urlPrefix);
            PointRDD spatialRDD = new PointRDD(sparkContext, earthdataInputLocation, earthdataNumPartitions, earthdataHDFPoint, StorageLevel.MEMORY_ONLY());
            ScatterPlot visualizationOperator = new ScatterPlot(1000, 600, spatialRDD.boundaryEnvelope, ColorizeOption.EARTHOBSERVATION, false, false);
            visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.BLUE, true);
            visualizationOperator.Visualize(sparkContext, spatialRDD);
            GeoSparkVizImageGenerator imageGenerator = new GeoSparkVizImageGenerator();
            imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.rasterImage, outputPath, ImageType.PNG);
        }
        catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * The main method.
     *
     * @param args the arguments
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public static void main(String[] args)
            throws IOException
    {
        SparkConf sparkConf = new SparkConf().setAppName("GeoSparkVizDemo").setMaster("local[4]");
        sparkContext = new JavaSparkContext(sparkConf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        prop = new Properties();

        String resourcePath = "src/test/resources/";
        String demoOutputPath = "target/demo";
        FileInputStream ConfFile = new FileInputStream(resourcePath + "babylon.point.properties");
        prop.load(ConfFile);

        String scatterPlotOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/scatterplot";
        String heatMapOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/heatmap";
        String choroplethMapOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/choroplethmap";
        String parallelFilterRenderStitchOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/parallelfilterrenderstitchheatmap";
        String earthdataScatterPlotOutputPath = System.getProperty("user.dir") + "/" + demoOutputPath + "/earthdatascatterplot";

        PointInputLocation = "file://" + System.getProperty("user.dir") + "/" + resourcePath + prop.getProperty("inputLocation");
        PointOffset = Integer.parseInt(prop.getProperty("offset"));
        ;
        PointSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
        PointNumPartitions = Integer.parseInt(prop.getProperty("numPartitions"));

        ConfFile = new FileInputStream(resourcePath + "babylon.rectangle.properties");
        prop.load(ConfFile);
        RectangleInputLocation = "file://" + System.getProperty("user.dir") + "/" + resourcePath + prop.getProperty("inputLocation");
        RectangleOffset = Integer.parseInt(prop.getProperty("offset"));
        RectangleSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
        RectangleNumPartitions = Integer.parseInt(prop.getProperty("numPartitions"));

        ConfFile = new FileInputStream(resourcePath + "babylon.polygon.properties");
        prop.load(ConfFile);
        PolygonInputLocation = "file://" + System.getProperty("user.dir") + "/" + resourcePath + prop.getProperty("inputLocation");
        PolygonOffset = Integer.parseInt(prop.getProperty("offset"));
        PolygonSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
        PolygonNumPartitions = Integer.parseInt(prop.getProperty("numPartitions"));

        ConfFile = new FileInputStream(resourcePath + "babylon.linestring.properties");
        prop.load(ConfFile);
        LineStringInputLocation = "file://" + System.getProperty("user.dir") + "/" + resourcePath + prop.getProperty("inputLocation");
        LineStringOffset = Integer.parseInt(prop.getProperty("offset"));
        LineStringSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
        LineStringNumPartitions = Integer.parseInt(prop.getProperty("numPartitions"));

        USMainLandBoundary = new Envelope(-126.790180, -64.630926, 24.863836, 50.000);

        earthdataInputLocation = System.getProperty("user.dir") + "/src/test/resources/modis/modis.csv";
        earthdataNumPartitions = 5;
        HDFIncrement = 5;
        HDFOffset = 2;
        HDFRootGroupName = "MOD_Swath_LST";
        HDFDataVariableName = "LST";
        HDFswitchXY = true;
        urlPrefix = System.getProperty("user.dir") + "/src/test/resources/modis/";

        if (buildScatterPlot(scatterPlotOutputPath) && buildHeatMap(heatMapOutputPath)
                && buildChoroplethMap(choroplethMapOutputPath) && parallelFilterRenderStitch(parallelFilterRenderStitchOutputPath + "-stitched")
                && parallelFilterRenderNoStitch(parallelFilterRenderStitchOutputPath) && earthdataVisualization(earthdataScatterPlotOutputPath)) {
            System.out.println("All 5 GeoSparkViz Demos have passed.");
        }
        else {
            System.out.println("GeoSparkViz Demos failed.");
        }
        sparkContext.stop();
    }
}
