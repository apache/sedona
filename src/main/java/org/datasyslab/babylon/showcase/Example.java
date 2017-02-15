/**
 * FILE: Example.java
 * PATH: org.datasyslab.babylon.showcase.Example.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.showcase;

import java.awt.Color;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.babylon.core.OverlayOperator;
import org.datasyslab.babylon.extension.imageGenerator.NativeJavaImageGenerator;
import org.datasyslab.babylon.extension.visualizationEffect.ChoroplethMap;
import org.datasyslab.babylon.extension.visualizationEffect.HeatMap;
import org.datasyslab.babylon.extension.visualizationEffect.ScatterPlot;
import org.datasyslab.babylon.utils.ImageType;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.spatialOperator.JoinQuery;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Polygon;

/**
 * The Class Example.
 */
public class Example {
	
	/** The spark context. */
	static JavaSparkContext sparkContext;
	
    /** The prop. */
    static Properties prop;
            
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
	 * Builds the scatter plot.
	 *
	 * @param outputPath the output path
	 * @return true, if successful
	 */
	public static boolean buildScatterPlot(String outputPath)
	{
		try{
			PolygonRDD spatialRDD = new PolygonRDD(sparkContext, PolygonInputLocation, PolygonSplitter, false, PolygonNumPartitions);
			ScatterPlot visualizationOperator = new ScatterPlot(1000,600,USMainLandBoundary,false);
			visualizationOperator.CustomizeColor(255, 255, 255, 255, Color.GREEN, true);
			visualizationOperator.Visualize(sparkContext, spatialRDD);
			NativeJavaImageGenerator imageGenerator = new  NativeJavaImageGenerator();
			imageGenerator.SaveAsFile(visualizationOperator.pixelImage, outputPath, ImageType.PNG);	
		}
		catch(Exception e)
		{
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
		try{
			RectangleRDD spatialRDD = new RectangleRDD(sparkContext, RectangleInputLocation, RectangleSplitter, false, RectangleNumPartitions);
			HeatMap visualizationOperator = new HeatMap(1000,600,USMainLandBoundary,false,2);
			visualizationOperator.Visualize(sparkContext, spatialRDD);
			NativeJavaImageGenerator imageGenerator = new  NativeJavaImageGenerator();
			imageGenerator.SaveAsFile(visualizationOperator.pixelImage, outputPath,ImageType.GIF);
		}
		catch(Exception e)
		{
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
		try{
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
			imageGenerator.SaveAsFile(overlayOperator.backImage, outputPath,ImageType.PNG);
		}
		catch(Exception e)
		{
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
		try{
			RectangleRDD spatialRDD = new RectangleRDD(sparkContext, RectangleInputLocation, RectangleSplitter, false, RectangleNumPartitions);
			HeatMap visualizationOperator = new HeatMap(1000,600,USMainLandBoundary,false,2,4,4,true,true);
			visualizationOperator.Visualize(sparkContext, spatialRDD);
			visualizationOperator.stitchImagePartitions();
			NativeJavaImageGenerator imageGenerator = new  NativeJavaImageGenerator();
			imageGenerator.SaveAsFile(visualizationOperator.pixelImage, outputPath,ImageType.GIF);
		}
		catch(Exception e)
		{
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
	public static void main(String[] args) throws IOException {
		SparkConf sparkConf = new SparkConf().setAppName("BabylonDemo").setMaster("local[4]");
		sparkContext = new JavaSparkContext(sparkConf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        prop = new Properties();
        
        String resourcePath = "src/test/resources/";
		FileInputStream ConfFile= new FileInputStream(resourcePath+"babylon.point.properties");
		prop.load(ConfFile);
        PointInputLocation = "file://"+System.getProperty("user.dir")+"/"+resourcePath+prop.getProperty("inputLocation");
        PointOffset = Integer.parseInt(prop.getProperty("offset"));;
        PointSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
        PointNumPartitions = Integer.parseInt(prop.getProperty("numPartitions"));
        
		ConfFile= new FileInputStream(resourcePath+"babylon.rectangle.properties");
		prop.load(ConfFile);
        RectangleInputLocation = "file://"+System.getProperty("user.dir")+"/"+resourcePath+prop.getProperty("inputLocation");
        RectangleOffset = Integer.parseInt(prop.getProperty("offset"));
        RectangleSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
        RectangleNumPartitions = Integer.parseInt(prop.getProperty("numPartitions"));
        
		ConfFile= new FileInputStream(resourcePath+"babylon.polygon.properties");
		prop.load(ConfFile);
        PolygonInputLocation = "file://"+System.getProperty("user.dir")+"/"+resourcePath+prop.getProperty("inputLocation");
        PolygonOffset = Integer.parseInt(prop.getProperty("offset"));
        PolygonSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
        PolygonNumPartitions = Integer.parseInt(prop.getProperty("numPartitions"));

		ConfFile= new FileInputStream(resourcePath+"babylon.linestring.properties");
		prop.load(ConfFile);
        LineStringInputLocation = "file://"+System.getProperty("user.dir")+"/"+resourcePath+prop.getProperty("inputLocation");
        LineStringOffset = Integer.parseInt(prop.getProperty("offset"));
        LineStringSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
        LineStringNumPartitions = Integer.parseInt(prop.getProperty("numPartitions"));
        
        USMainLandBoundary = new Envelope(-126.790180,-64.630926,24.863836,50.000);

        if(buildScatterPlot(args[0])&&buildHeatMap(args[1])&&buildChoroplethMap(args[2])&&parallelFilterRenderStitch(args[3]))
        {
        	System.out.println("All 4 Babylon Demos have passed.");
        }
        else
        {
        	System.out.println("Babylon Demos failed.");

        }
        sparkContext.stop();
	}

}
