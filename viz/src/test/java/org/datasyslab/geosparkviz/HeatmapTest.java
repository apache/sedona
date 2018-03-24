/**
 * FILE: HeatmapTest.java
 * PATH: org.datasyslab.geosparkviz.HeatmapTest.java
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
 * The Class HeatmapTest.
 */
public class HeatmapTest extends GeoSparkVizTestBase{
    
	/**
	 * Sets the up before class.
	 *
	 * @throws Exception the exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		initialize(HeatmapTest.class.getSimpleName());
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
		HeatMap visualizationOperator = new HeatMap(800,500,USMainLandBoundary,false,3);
		visualizationOperator.Visualize(sparkContext, spatialRDD);
		ImageGenerator imageGenerator = new  ImageGenerator();
		imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.rasterImage, "./target/heatmap/PointRDD", ImageType.PNG);
	}
	
	/**
	 * Test rectangle RDD visualization.
	 *
	 * @throws Exception the exception
	 */
	@Test
	public void testRectangleRDDVisualization() throws Exception {
		RectangleRDD spatialRDD = new RectangleRDD(sparkContext, RectangleInputLocation, RectangleSplitter, false, RectangleNumPartitions, StorageLevel.MEMORY_ONLY());
		HeatMap visualizationOperator = new HeatMap(800,500,USMainLandBoundary,false,2, 4,4,false,true);
		visualizationOperator.Visualize(sparkContext, spatialRDD);
		
		ImageGenerator imageGenerator = new ImageGenerator();
		imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.distributedRasterImage, "./target/heatmap/RectangleRDD",ImageType.PNG, 0, 4, 4);
		ImageStitcher.stitchImagePartitionsFromLocalFile("./target/heatmap/RectangleRDD", 800, 500,0,4,4);
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
		HeatMap visualizationOperator = new HeatMap(800,500,USMainLandBoundary,false,2);
		visualizationOperator.Visualize(sparkContext, spatialRDD);
		
		ImageGenerator imageGenerator = new ImageGenerator();
		imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.rasterImage, "./target/heatmap/PolygonRDD",ImageType.PNG);

	}
	
	/**
	 * Test line string RDD visualization.
	 *
	 * @throws Exception the exception
	 */
	@Test
	public void testLineStringRDDVisualization() throws Exception {
		LineStringRDD spatialRDD = new LineStringRDD(sparkContext, LineStringInputLocation, LineStringSplitter, false, LineStringNumPartitions, StorageLevel.MEMORY_ONLY());
		HeatMap visualizationOperator = new HeatMap(800,500,USMainLandBoundary,false,2);
		visualizationOperator.Visualize(sparkContext, spatialRDD);
		
		ImageGenerator imageGenerator = new ImageGenerator();
		imageGenerator.SaveRasterImageAsLocalFile(visualizationOperator.rasterImage, "./target/heatmap/LineStringRDD",ImageType.PNG);
	}
}
