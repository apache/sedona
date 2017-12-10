/**
 * FILE: ParallelVisualizationTest.java
 * PATH: org.datasyslab.geosparkviz.ParallelVisualizationTest.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geosparkviz;

import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geosparkviz.core.ImageGenerator;
import org.datasyslab.geosparkviz.core.ImageStitcher;
import org.datasyslab.geosparkviz.extension.visualizationEffect.HeatMap;
import org.datasyslab.geosparkviz.utils.ImageType;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.spatialRDD.LineStringRDD;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vividsolutions.jts.geom.Envelope;

// TODO: Auto-generated Javadoc
/**
 * The Class ParallelVisualizationTest.
 */
public class ParallelVisualizationTest {

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
    
    /** The resolution X. */
    static int resolutionX;
    
    /** The resolution Y. */
    static int resolutionY;
    
    /** The partition X. */
    static int partitionX;
    
    /** The partition Y. */
    static int partitionY;
	
	/**
	 * Sets the up before class.
	 *
	 * @throws Exception the exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		SparkConf sparkConf = new SparkConf().setAppName("ParallelVisualizationTest").setMaster("local[4]");
		sparkContext = new JavaSparkContext(sparkConf);
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.datasyslab").setLevel(Level.DEBUG);
        Logger.getLogger("akka").setLevel(Level.WARN);
        prop = new Properties();
        
        inputProp = ParallelVisualizationTest.class.getClassLoader().getResourceAsStream("babylon.point.properties");
        prop.load(inputProp);
        PointInputLocation = "file://"+ParallelVisualizationTest.class.getClassLoader().getResource(prop.getProperty("inputLocation")).getPath();
        PointOffset = Integer.parseInt(prop.getProperty("offset"));;
        PointSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
        PointNumPartitions = Integer.parseInt(prop.getProperty("numPartitions"));
        
        inputProp = ParallelVisualizationTest.class.getClassLoader().getResourceAsStream("babylon.rectangle.properties");
        prop.load(inputProp);
        RectangleInputLocation = "file://"+ParallelVisualizationTest.class.getClassLoader().getResource(prop.getProperty("inputLocation")).getPath();
        RectangleOffset = Integer.parseInt(prop.getProperty("offset"));
        RectangleSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
        RectangleNumPartitions = Integer.parseInt(prop.getProperty("numPartitions"));
        
        inputProp = ParallelVisualizationTest.class.getClassLoader().getResourceAsStream("babylon.polygon.properties");
        prop.load(inputProp);
        PolygonInputLocation = "file://"+ParallelVisualizationTest.class.getClassLoader().getResource(prop.getProperty("inputLocation")).getPath();
        PolygonOffset = Integer.parseInt(prop.getProperty("offset"));
        PolygonSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
        PolygonNumPartitions = Integer.parseInt(prop.getProperty("numPartitions"));

        inputProp = ParallelVisualizationTest.class.getClassLoader().getResourceAsStream("babylon.linestring.properties");
        prop.load(inputProp);
        LineStringInputLocation = "file://"+ParallelVisualizationTest.class.getClassLoader().getResource(prop.getProperty("inputLocation")).getPath();
        LineStringOffset = Integer.parseInt(prop.getProperty("offset"));
        LineStringSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
        LineStringNumPartitions = Integer.parseInt(prop.getProperty("numPartitions"));
        
        USMainLandBoundary = new Envelope(-126.790180,-64.630926,24.863836,50.000);
        
        resolutionX = 1000;
        
        resolutionY = 600;
        
        partitionX = 2;
        
        partitionY = 2;
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
	 * Test point RDD visualization.
	 *
	 * @throws Exception the exception
	 */
	@Test
	public void testPointRDDVisualization() throws Exception {
		PointRDD spatialRDD = new PointRDD(sparkContext, PointInputLocation, PointOffset, PointSplitter, false, PointNumPartitions, StorageLevel.MEMORY_ONLY());
		HeatMap visualizationOperator = new HeatMap(resolutionX,resolutionY,USMainLandBoundary,false,2,partitionX,partitionY,true,true);
		visualizationOperator.Visualize(sparkContext, spatialRDD);

		ImageGenerator imageGenerator = new  ImageGenerator();
		imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.distributedRasterImage, "./target/parallelvisualization/PointRDD",ImageType.PNG,0,partitionX, partitionY);
        ImageStitcher.stitchImagePartitionsFromLocalFile("./target/parallelvisualization/PointRDD", resolutionX,resolutionY,0,partitionX, partitionY);
    }
	
	/**
	 * Test rectangle RDD visualization with tiles.
	 *
	 * @throws Exception the exception
	 */
	@Test
	public void testRectangleRDDVisualizationWithTiles() throws Exception {
		RectangleRDD spatialRDD = new RectangleRDD(sparkContext, RectangleInputLocation, RectangleSplitter, false, RectangleNumPartitions, StorageLevel.MEMORY_ONLY());
		HeatMap visualizationOperator = new HeatMap(resolutionX,resolutionY,USMainLandBoundary,false,2,partitionX,partitionY,true,true);
		visualizationOperator.Visualize(sparkContext, spatialRDD);

		ImageGenerator imageGenerator = new  ImageGenerator();
		imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.distributedRasterImage, "./target/parallelvisualization/RectangleRDDWithTiles",ImageType.PNG,0,partitionX, partitionY);
        ImageStitcher.stitchImagePartitionsFromLocalFile("./target/parallelvisualization/RectangleRDDWithTiles", resolutionX,resolutionY,0,partitionX, partitionY);
	}

    /**
     * Test rectangle RDD visualization no tiles.
     *
     * @throws Exception the exception
     */
    @Test
    public void testRectangleRDDVisualizationNoTiles() throws Exception {
        RectangleRDD spatialRDD = new RectangleRDD(sparkContext, RectangleInputLocation, RectangleSplitter, false, RectangleNumPartitions, StorageLevel.MEMORY_ONLY());
        HeatMap visualizationOperator = new HeatMap(resolutionX,resolutionY,USMainLandBoundary,false,5,partitionX,partitionY,true,true);
        visualizationOperator.Visualize(sparkContext, spatialRDD);

        ImageGenerator imageGenerator = new  ImageGenerator();
		imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.distributedRasterImage, "./target/parallelvisualization/RectangleRDDNoTiles",ImageType.PNG, 0, partitionX, partitionY);
		ImageStitcher.stitchImagePartitionsFromLocalFile("./target/parallelvisualization/RectangleRDDNoTiles", resolutionX,resolutionY,0,partitionX, partitionY);
    }

	/**
	 * Test polygon RDD visualization.
	 *
	 * @throws Exception the exception
	 */
	@Test
	public void testPolygonRDDVisualization() throws Exception {
		//UserSuppliedPolygonMapper userSuppliedPolygonMapper = new UserSuppliedPolygonMapper();
		PolygonRDD spatialRDD = new PolygonRDD(sparkContext, PolygonInputLocation, PolygonSplitter, false, PolygonNumPartitions, StorageLevel.MEMORY_ONLY());
		HeatMap visualizationOperator = new HeatMap(resolutionX,resolutionY,USMainLandBoundary,false,2,partitionX,partitionY,true,true);
		visualizationOperator.Visualize(sparkContext, spatialRDD);
		
		ImageGenerator imageGenerator = new ImageGenerator();
		imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.distributedRasterImage, "./target/parallelvisualization/PolygonRDD",ImageType.PNG, 0, partitionX, partitionY);
		ImageStitcher.stitchImagePartitionsFromLocalFile("./target/parallelvisualization/PolygonRDD", resolutionX,resolutionY,0,partitionX, partitionY);
		
	}
	
	/**
	 * Test line string RDD visualization.
	 *
	 * @throws Exception the exception
	 */
	@Test
	public void testLineStringRDDVisualization() throws Exception {
		LineStringRDD spatialRDD = new LineStringRDD(sparkContext, LineStringInputLocation, LineStringSplitter, false, LineStringNumPartitions, StorageLevel.MEMORY_ONLY());
		HeatMap visualizationOperator = new HeatMap(resolutionX,resolutionY,USMainLandBoundary,false,2,partitionX,partitionY,true,true);
		visualizationOperator.Visualize(sparkContext, spatialRDD);
		
		ImageGenerator imageGenerator = new ImageGenerator();
		imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.distributedRasterImage, "./target/parallelvisualization/LineStringRDD",ImageType.PNG, 0, partitionX, partitionY);
		ImageStitcher.stitchImagePartitionsFromLocalFile("./target/parallelvisualization/LineStringRDD", resolutionX,resolutionY,0,partitionX, partitionY);
	}
}
