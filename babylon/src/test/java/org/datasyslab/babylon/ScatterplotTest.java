/**
 * FILE: ScatterplotTest.java
 * PATH: org.datasyslab.babylon.ScatterplotTest.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon;

import java.awt.Color;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.babylon.extension.imageGenerator.BabylonImageGenerator;
import org.datasyslab.babylon.extension.visualizationEffect.ScatterPlot;
import org.datasyslab.babylon.utils.ColorizeOption;
import org.datasyslab.babylon.utils.ImageType;
import org.datasyslab.babylon.utils.RasterizationUtils;
import org.datasyslab.geospark.enums.FileDataSplitter;

import org.datasyslab.geospark.spatialRDD.LineStringRDD;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vividsolutions.jts.geom.Envelope;


import scala.Tuple2;


/**
 * The Class ScatterplotTest.
 */
public class ScatterplotTest {
	
    /** The spark context. */
    static JavaSparkContext sparkContext;
    
    /** The prop. */
    static Properties prop;
    
    /** The input prop. */
    static InputStream inputProp;
    
    /** The Point input location. */
    static String PointInputLocation;
    
    /** The Point offset. */
    static Integer PointOffset;
    
    /** The Point splitter. */
    static FileDataSplitter PointSplitter;
    
    /** The Point num partitions. */
    static Integer PointNumPartitions;
    
    /** The Rectangle input location. */
    static String RectangleInputLocation;
    
    /** The Rectangle offset. */
    static Integer RectangleOffset;
    
    /** The Rectangle splitter. */
    static FileDataSplitter RectangleSplitter;
    
    /** The Rectangle num partitions. */
    static Integer RectangleNumPartitions;
    
    /** The Polygon input location. */
    static String PolygonInputLocation;
    
    /** The Polygon offset. */
    static Integer PolygonOffset;
    
    /** The Polygon splitter. */
    static FileDataSplitter PolygonSplitter;
    
    /** The Polygon num partitions. */
    static Integer PolygonNumPartitions;
    
    /** The Line string input location. */
    static String LineStringInputLocation;
    
    /** The Line string offset. */
    static Integer LineStringOffset;
    
    /** The Line string splitter. */
    static FileDataSplitter LineStringSplitter;
    
    /** The Line string num partitions. */
    static Integer LineStringNumPartitions;
    
    /** The US main land boundary. */
    static Envelope USMainLandBoundary;
    
	/**
	 * Sets the up before class.
	 *
	 * @throws Exception the exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		SparkConf sparkConf = new SparkConf().setAppName("ScatterplotTest").setMaster("local[4]");
		sparkContext = new JavaSparkContext(sparkConf);
        Logger.getLogger("org.apache").setLevel(Level.WARN);
		Logger.getLogger("org.datasyslab").setLevel(Level.INFO);
		Logger.getLogger("akka").setLevel(Level.WARN);
        prop = new Properties();
        
        inputProp = ScatterplotTest.class.getClassLoader().getResourceAsStream("babylon.point.properties");
        prop.load(inputProp);
        PointInputLocation = "file://"+ScatterplotTest.class.getClassLoader().getResource(prop.getProperty("inputLocation")).getPath();
        PointOffset = Integer.parseInt(prop.getProperty("offset"));;
        PointSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
        PointNumPartitions = Integer.parseInt(prop.getProperty("numPartitions"));
        
        inputProp = ScatterplotTest.class.getClassLoader().getResourceAsStream("babylon.rectangle.properties");
        prop.load(inputProp);
        RectangleInputLocation = "file://"+ScatterplotTest.class.getClassLoader().getResource(prop.getProperty("inputLocation")).getPath();
        RectangleOffset = Integer.parseInt(prop.getProperty("offset"));
        RectangleSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
        RectangleNumPartitions = Integer.parseInt(prop.getProperty("numPartitions"));
        
        inputProp = ScatterplotTest.class.getClassLoader().getResourceAsStream("babylon.polygon.properties");
        prop.load(inputProp);
        PolygonInputLocation = "file://"+ScatterplotTest.class.getClassLoader().getResource(prop.getProperty("inputLocation")).getPath();
        PolygonOffset = Integer.parseInt(prop.getProperty("offset"));
        PolygonSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
        PolygonNumPartitions = Integer.parseInt(prop.getProperty("numPartitions"));

        inputProp = ScatterplotTest.class.getClassLoader().getResourceAsStream("babylon.linestring.properties");
        prop.load(inputProp);
        LineStringInputLocation = "file://"+ScatterplotTest.class.getClassLoader().getResource(prop.getProperty("inputLocation")).getPath();
        LineStringOffset = Integer.parseInt(prop.getProperty("offset"));
        LineStringSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
        LineStringNumPartitions = Integer.parseInt(prop.getProperty("numPartitions"));
        
        USMainLandBoundary = new Envelope(-126.790180,-64.630926,24.863836,50.000);
	}

	/**
	 * Tear down.
	 *
	 * @throws Exception the exception
	 */
	@AfterClass
	public static void tearDown() throws Exception {
		sparkContext.stop();
	}

	/**
	 * Test encode decode id.
	 *
	 * @throws Exception the exception
	 */
	@Test
	public void testEncodeDecodeId() throws Exception {
		int pixelX = 33;
		int pixelY = 44;
		int resolutionX = 1000;
		int resolutionY = 600;
		int serialId = RasterizationUtils.Encode2DTo1DId(resolutionX, resolutionY, pixelX, pixelY);
		Tuple2<Integer, Integer> pixelCoordinate = RasterizationUtils.Decode1DTo2DId(resolutionX, resolutionY, serialId);
		assert pixelCoordinate._1 == pixelX;
		assert pixelCoordinate._2 == pixelY;
		
		pixelX = 1000;
		pixelY = 2;
		resolutionX = 2000;
		resolutionY = 1200;
		serialId = RasterizationUtils.Encode2DTo1DId(resolutionX, resolutionY, pixelX, pixelY);
		pixelCoordinate = RasterizationUtils.Decode1DTo2DId(resolutionX, resolutionY, serialId);
		assert pixelCoordinate._1 == pixelX;
		assert pixelCoordinate._2 == pixelY;
	}
	
	/**
	 * Test point RDD visualization.
	 *
	 * @throws Exception the exception
	 */
	@Test
	public void testPointRDDVisualization() throws Exception {
		PointRDD spatialRDD = new PointRDD(sparkContext, PointInputLocation, PointOffset, PointSplitter, false, PointNumPartitions);
		ScatterPlot visualizationOperator = new ScatterPlot(1000,600,USMainLandBoundary,false);
		visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.GREEN, true);
		visualizationOperator.Visualize(sparkContext, spatialRDD);
		BabylonImageGenerator imageGenerator = new  BabylonImageGenerator();
		imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.rasterImage, "./target/scatterplot/PointRDD",ImageType.PNG);
		
		visualizationOperator = new ScatterPlot(1000,600,USMainLandBoundary,false,true);
		visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.GREEN, true);
		visualizationOperator.Visualize(sparkContext, spatialRDD);
		imageGenerator = new  BabylonImageGenerator();
		imageGenerator.SaveVectorImageAsLocalFile(visualizationOperator.vectorImage, "./target/scatterplot/PointRDD",ImageType.SVG);
	}

	/**
	 * Test point RDD visualization.
	 *
	 * @throws Exception the exception
	 */
	@Test
	public void testPointRDDVisualizationWithParallelRendering() throws Exception {
		PointRDD spatialRDD = new PointRDD(sparkContext, PointInputLocation, PointOffset, PointSplitter, false, PointNumPartitions);
		ScatterPlot visualizationOperator = new ScatterPlot(1000,600,USMainLandBoundary, ColorizeOption.UNIFORMCOLOR,
				false, 4, 4, true, false);
		visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.GREEN, true);
		visualizationOperator.Visualize(sparkContext, spatialRDD);
		visualizationOperator.stitchImagePartitions();
		BabylonImageGenerator imageGenerator = new  BabylonImageGenerator();
		imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.rasterImage, "./target/scatterplot/PointRDD-parallelrender",ImageType.PNG);
		//imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.distributedRasterImage, "./target/scatterplot/PointRDD-parallelrender",ImageType.PNG);
	}
	
	/**
	 * Test point RDD visualization.
	 *
	 * @throws Exception the exception
	 */
	@Test
	public void testSaveAsDistributedFile() throws Exception {
		PointRDD spatialRDD = new PointRDD(sparkContext, PointInputLocation, PointOffset, PointSplitter, false, PointNumPartitions);
		ScatterPlot visualizationOperator = new ScatterPlot(1000,600,USMainLandBoundary,false,2,2,true,false);
		visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.GREEN, true);
		visualizationOperator.Visualize(sparkContext, spatialRDD);
		BabylonImageGenerator imageGenerator = new  BabylonImageGenerator();
		
        String scatterPlotOutputPath = System.getProperty("user.dir") + "/target/scatterplot/";

		imageGenerator.SaveRasterImageAsSparkFile(visualizationOperator.distributedRasterImage, scatterPlotOutputPath+"PointRDD-parallel-raster",ImageType.PNG);
		
		visualizationOperator = new ScatterPlot(1000,600,USMainLandBoundary,false,-1,-1,true,true);
		visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.GREEN, true);
		visualizationOperator.Visualize(sparkContext, spatialRDD);
		imageGenerator = new  BabylonImageGenerator();
		imageGenerator.SaveVectorImageAsSparkFile(visualizationOperator.distributedVectorImage, scatterPlotOutputPath+"PointRDD-parallel-vector",ImageType.SVG);
	}
	
	/**
	 * Test rectangle RDD visualization.
	 *
	 * @throws Exception the exception
	 */
	@Test
	public void testRectangleRDDVisualization() throws Exception {
		RectangleRDD spatialRDD = new RectangleRDD(sparkContext, RectangleInputLocation, RectangleSplitter, false, RectangleNumPartitions);
		ScatterPlot visualizationOperator = new ScatterPlot(1000,600,USMainLandBoundary,false);
		visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.RED, true);
		visualizationOperator.Visualize(sparkContext, spatialRDD);
		BabylonImageGenerator imageGenerator = new  BabylonImageGenerator();
		imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.rasterImage, "./target/scatterplot/RectangleRDD",ImageType.GIF);
		
		visualizationOperator = new ScatterPlot(1000,600,USMainLandBoundary,false,true);
		visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.RED, true);
		visualizationOperator.Visualize(sparkContext, spatialRDD);
		imageGenerator = new  BabylonImageGenerator();
		imageGenerator.SaveVectorImageAsLocalFile(visualizationOperator.vectorImage, "./target/scatterplot/RectangleRDD",ImageType.SVG);	
	}
	
	/**
	 * Test polygon RDD visualization.
	 *
	 * @throws Exception the exception
	 */
	@Test
	public void testPolygonRDDVisualization() throws Exception {
		//UserSuppliedPolygonMapper userSuppliedPolygonMapper = new UserSuppliedPolygonMapper();
		PolygonRDD spatialRDD = new PolygonRDD(sparkContext, PolygonInputLocation, PolygonSplitter, false, PolygonNumPartitions);
		ScatterPlot visualizationOperator = new ScatterPlot(1000,600,USMainLandBoundary,false);
		visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.GREEN, true);
		visualizationOperator.Visualize(sparkContext, spatialRDD);
		BabylonImageGenerator imageGenerator = new  BabylonImageGenerator();
		imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.rasterImage, "./target/scatterplot/PolygonRDD",ImageType.GIF);
		
		visualizationOperator = new ScatterPlot(1000,600,USMainLandBoundary,false,true);
		visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.GREEN, true);
		visualizationOperator.Visualize(sparkContext, spatialRDD);
		imageGenerator = new  BabylonImageGenerator();
		imageGenerator.SaveVectorImageAsLocalFile(visualizationOperator.vectorImage, "./target/scatterplot/PolygonRDD",ImageType.SVG);
	}
	
	/**
	 * Test line string RDD visualization.
	 *
	 * @throws Exception the exception
	 */
	@Test
	public void testLineStringRDDVisualization() throws Exception {
		int resolutionY = 800;
		int resolutionX = RasterizationUtils.GetWidthFromHeight(resolutionY, USMainLandBoundary);
		//UserSuppliedLineStringMapper userSuppliedLineStringMapper = new UserSuppliedLineStringMapper();
		LineStringRDD spatialRDD = new LineStringRDD(sparkContext, LineStringInputLocation, LineStringSplitter, false, LineStringNumPartitions);
		ScatterPlot visualizationOperator = new ScatterPlot(resolutionX,resolutionY,USMainLandBoundary,false);
		visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.GREEN, true);
		visualizationOperator.Visualize(sparkContext, spatialRDD);
		BabylonImageGenerator imageGenerator = new  BabylonImageGenerator();
		imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.rasterImage, "./target/scatterplot/LineStringRDD",ImageType.GIF);
		
		visualizationOperator = new ScatterPlot(resolutionX,resolutionY,USMainLandBoundary,false,true);
		visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.GREEN, true);
		visualizationOperator.Visualize(sparkContext, spatialRDD);
		imageGenerator = new  BabylonImageGenerator();
		imageGenerator.SaveVectorImageAsLocalFile(visualizationOperator.vectorImage, "./target/scatterplot/LineStringRDD",ImageType.SVG);
	}
}
