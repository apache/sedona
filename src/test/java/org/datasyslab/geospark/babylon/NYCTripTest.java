/**
 * FILE: NYCTripTest.java
 * PATH: org.datasyslab.geospark.babylon.NYCTripTest.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.babylon;

import java.awt.Color;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.datasyslab.babylon.extension.imageGenerator.NativeJavaImageGenerator;
import org.datasyslab.babylon.extension.visualizationEffect.HeatMap;
import org.datasyslab.babylon.utils.ImageType;
import org.datasyslab.babylon.utils.RasterizationUtils;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.showcase.UserSuppliedPolygonMapper;
import org.datasyslab.geospark.spatialRDD.LineStringRDD;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKTReader;

class NYCTripMapper implements FlatMapFunction<String, Object> {
	 
	List result= new ArrayList<Polygon>();
    Geometry spatialObject = null;
    MultiPolygon multiSpatialObjects = null;
    GeometryFactory fact = new GeometryFactory();
    List<String> lineSplitList;
    ArrayList<Coordinate> coordinatesList;
    Coordinate[] coordinates;
    LinearRing linear;
    int actualEndOffset;
    public Iterator call(String line) throws Exception {
        List result= new ArrayList<LineString>();
        Geometry spatialObject = null;
        MultiLineString multiSpatialObjects = null;
        List<String> lineSplitList;
        lineSplitList = Arrays.asList(line.split(","));
        coordinatesList = new ArrayList<Coordinate>();
        coordinatesList.add(new Coordinate(Double.parseDouble(lineSplitList.get(5))*-1.0, Double.parseDouble(lineSplitList.get(6))));
        coordinatesList.add(new Coordinate(Double.parseDouble(lineSplitList.get(8))*-1.0, Double.parseDouble(lineSplitList.get(9))));
        spatialObject = fact.createLineString(coordinatesList.toArray(new Coordinate[coordinatesList.size()]));
        result.add((LineString)spatialObject);
        return result.iterator();
    }

} 

/**
 * The Class NYCTripTest.
 */
public class NYCTripTest {

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
    
    /** The NYC boundary. */
    static Envelope NYCBoundary;
    
	/**
	 * Sets the up before class.
	 *
	 * @throws Exception the exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		SparkConf sparkConf = new SparkConf().setAppName("HeatmapTest").setMaster("local[4]");
		sparkContext = new JavaSparkContext(sparkConf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        prop = new Properties();
        inputProp = ScatterplotTest.class.getClassLoader().getResourceAsStream("point.test.properties");
        prop.load(inputProp);
        PointInputLocation = "file://"+ScatterplotTest.class.getClassLoader().getResource("tweets.tsv").getPath();
        PointOffset = 1;
        PointSplitter = FileDataSplitter.WKT;
        PointNumPartitions = Integer.parseInt(prop.getProperty("numPartitions"));
        
        inputProp = ScatterplotTest.class.getClassLoader().getResourceAsStream("rectangle.test.properties");
        prop.load(inputProp);
        RectangleInputLocation = "file://"+ScatterplotTest.class.getClassLoader().getResource("zcta510.csv").getPath();
        RectangleOffset = Integer.parseInt(prop.getProperty("offset"));
        RectangleSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
        RectangleNumPartitions = Integer.parseInt(prop.getProperty("numPartitions"));
        
        inputProp = ScatterplotTest.class.getClassLoader().getResourceAsStream("polygon.test.properties");
        prop.load(inputProp);
        PolygonInputLocation = "file://"+ScatterplotTest.class.getClassLoader().getResource("county.csv").getPath();
        PolygonOffset = Integer.parseInt(prop.getProperty("offset"));
        PolygonSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
        PolygonNumPartitions = Integer.parseInt(prop.getProperty("numPartitions"));

        inputProp = ScatterplotTest.class.getClassLoader().getResourceAsStream("linestring.test.properties");
        prop.load(inputProp);
        LineStringInputLocation = "file://"+ScatterplotTest.class.getClassLoader().getResource("trip-sample.csv").getPath();
        LineStringOffset = Integer.parseInt(prop.getProperty("offset"));
        LineStringSplitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
        LineStringNumPartitions = 8;
        
        NYCBoundary = new Envelope(-74.25,-73.7,40.5,40.9);
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
	 * Test line string RDD visualization.
	 *
	 * @throws Exception the exception
	 */
	public void testLineStringRDDVisualization() throws Exception {
		
		int resolutionY = 800;
		int resolutionX = RasterizationUtils.GetWidthFromHeight(resolutionY, NYCBoundary);
		
		NYCTripMapper nycTripMapper = new NYCTripMapper();
		UserSuppliedPolygonMapper userSuppliedPolygonMapper = new UserSuppliedPolygonMapper();
		
		LineStringRDD spatialRDD = new LineStringRDD(sparkContext, LineStringInputLocation, LineStringNumPartitions, nycTripMapper);
		HeatMap visualizationOperator = new HeatMap(resolutionX,resolutionY,NYCBoundary,false,2);
		visualizationOperator.Visualize(sparkContext, spatialRDD);
		
		NativeJavaImageGenerator imageGenerator = new  NativeJavaImageGenerator();
		imageGenerator.SaveAsFile(visualizationOperator.pixelImage, "./target/heatmap/NYCTrip", ImageType.PNG);	
	}
}
