/**
 * FILE: ChoroplethmapTest.java
 * PATH: org.datasyslab.geospark.babylon.ChoroplethmapTest.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.babylon;

import java.awt.Color;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.babylon.core.OverlayOperator;
import org.datasyslab.babylon.extension.imageGenerator.NativeJavaImageGenerator;
import org.datasyslab.babylon.extension.visualizationEffect.ChoroplethMap;
import org.datasyslab.babylon.extension.visualizationEffect.ScatterPlot;
import org.datasyslab.babylon.utils.ImageType;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.showcase.UserSuppliedPolygonMapper;
import org.datasyslab.geospark.spatialOperator.JoinQuery;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Polygon;

/**
 * The Class ChoroplethmapTest.
 */
public class ChoroplethmapTest implements Serializable{
	
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
		SparkConf sparkConf = new SparkConf().setAppName("ChoroplethmapTest").setMaster("local[4]");
		sparkContext = new JavaSparkContext(sparkConf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        prop = new Properties();
        
        inputProp = ChoroplethmapTest.class.getClassLoader().getResourceAsStream("babylon.point.properties");
        prop.load(inputProp);
        PointInputLocation = "file://"+ChoroplethmapTest.class.getClassLoader().getResource(prop.getProperty("inputLocation")).getPath();
        PointOffset = Integer.parseInt(prop.getProperty("offset"));;
        PointSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
        PointNumPartitions = Integer.parseInt(prop.getProperty("numPartitions"));
        
        inputProp = ChoroplethmapTest.class.getClassLoader().getResourceAsStream("babylon.rectangle.properties");
        prop.load(inputProp);
        RectangleInputLocation = "file://"+ChoroplethmapTest.class.getClassLoader().getResource(prop.getProperty("inputLocation")).getPath();
        RectangleOffset = Integer.parseInt(prop.getProperty("offset"));
        RectangleSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
        RectangleNumPartitions = Integer.parseInt(prop.getProperty("numPartitions"));
        
        inputProp = ChoroplethmapTest.class.getClassLoader().getResourceAsStream("babylon.polygon.properties");
        prop.load(inputProp);
        PolygonInputLocation = "file://"+ChoroplethmapTest.class.getClassLoader().getResource(prop.getProperty("inputLocation")).getPath();
        PolygonOffset = Integer.parseInt(prop.getProperty("offset"));
        PolygonSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
        PolygonNumPartitions = Integer.parseInt(prop.getProperty("numPartitions"));

        inputProp = ChoroplethmapTest.class.getClassLoader().getResourceAsStream("babylon.linestring.properties");
        prop.load(inputProp);
        LineStringInputLocation = "file://"+ChoroplethmapTest.class.getClassLoader().getResource(prop.getProperty("inputLocation")).getPath();
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
	 * Test rectangle RDD visualization.
	 *
	 * @throws Exception the exception
	 */
	@Test
	public void testRectangleRDDVisualization() throws Exception {
		PointRDD spatialRDD = new PointRDD(sparkContext, PointInputLocation, PointOffset, PointSplitter, false, PointNumPartitions);
		RectangleRDD queryRDD = new RectangleRDD(sparkContext, RectangleInputLocation, RectangleSplitter, false, RectangleNumPartitions);
		spatialRDD.spatialPartitioning(GridType.RTREE);
		queryRDD.spatialPartitioning(spatialRDD.grids);
  		spatialRDD.buildIndex(IndexType.RTREE,true);
  		JavaPairRDD<Envelope,Long> joinResult = JoinQuery.SpatialJoinQueryCountByKey(spatialRDD,queryRDD,true);
  		
  		ChoroplethMap visualizationOperator = new ChoroplethMap(1000,600,USMainLandBoundary,false);
		visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.RED, true);
		visualizationOperator.Visualize(sparkContext, joinResult);
		
		ScatterPlot frontImage = new ScatterPlot(1000,600,USMainLandBoundary,false);
		frontImage.CustomizeColor(0, 0, 0, 255, Color.GREEN, true);
		frontImage.Visualize(sparkContext, queryRDD);

		
		OverlayOperator overlayOperator = new OverlayOperator(visualizationOperator.pixelImage);
		overlayOperator.JoinImage(frontImage.pixelImage);
		
		NativeJavaImageGenerator imageGenerator = new NativeJavaImageGenerator();
		imageGenerator.SaveAsFile(overlayOperator.backImage,"./target/choroplethmap/RectangleRDD-combined", ImageType.PNG);
	}
	
	/**
	 * Test polygon RDD visualization.
	 *
	 * @throws Exception the exception
	 */
	@Test
	public void testPolygonRDDVisualization() throws Exception {
		//UserSuppliedPolygonMapper userSuppliedPolygonMapper = new UserSuppliedPolygonMapper();
		PointRDD spatialRDD = new PointRDD(sparkContext, PointInputLocation, PointOffset, PointSplitter, false, PointNumPartitions);
		PolygonRDD queryRDD = new PolygonRDD(sparkContext, PolygonInputLocation,  PolygonSplitter, false, PolygonNumPartitions);
		spatialRDD.spatialPartitioning(GridType.RTREE);
		queryRDD.spatialPartitioning(spatialRDD.grids);
  		spatialRDD.buildIndex(IndexType.RTREE,true);
		JavaPairRDD<Polygon,Long> joinResult = JoinQuery.SpatialJoinQueryCountByKey(spatialRDD,queryRDD,true);

		ChoroplethMap visualizationOperator = new ChoroplethMap(1000,600,USMainLandBoundary,false);
		visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.RED, true);
		visualizationOperator.Visualize(sparkContext, joinResult);
		
		ScatterPlot frontImage = new ScatterPlot(1000,600,USMainLandBoundary,false);
		frontImage.CustomizeColor(0, 0, 0, 255, Color.GREEN, true);
		frontImage.Visualize(sparkContext, queryRDD);
		
		OverlayOperator overlayOperator = new OverlayOperator(visualizationOperator.pixelImage);
		overlayOperator.JoinImage(frontImage.pixelImage);
		
		NativeJavaImageGenerator imageGenerator = new NativeJavaImageGenerator();
		imageGenerator.SaveAsFile(overlayOperator.backImage, "./target/choroplethmap/PolygonRDD-combined", ImageType.GIF);
		
	}

}
