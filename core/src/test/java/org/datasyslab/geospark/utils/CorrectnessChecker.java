/**
 * FILE: CorrectnessChecker.java
 * PATH: org.datasyslab.geospark.utils.CorrectnessChecker.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.spatialOperator.JoinQuery;
import org.datasyslab.geospark.spatialRDD.CircleRDD;
import org.datasyslab.geospark.spatialRDD.LineStringRDD;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

import scala.Tuple2;

// TODO: Auto-generated Javadoc
class PointByPolygonSortComparator implements Comparator<Tuple2<Polygon,HashSet<Point>>>
{

	@Override
	public int compare(Tuple2<Polygon, HashSet<Point>> o1, Tuple2<Polygon, HashSet<Point>> o2) {
		Polygon p1 = o1._1;
		Polygon p2 = o2._1;
		Coordinate c1 = p1.getCoordinate();
		Coordinate c2 = p2.getCoordinate();
		if(c1.y > c2.y)
		{
			return 1;
		}
		else if(c1.y<c2.y)
		{
			return -1;
		}
		else
		{
			if(c1.x > c2.x)
			{
				return 1;
			}
			else if(c1.x < c2.x )
			{
				return -1;
			}
			else return 0;
		}
		
	}
}

class PolygonByPolygonSortComparator implements Comparator<Tuple2<Polygon,HashSet<Polygon>>>
{

	@Override
	public int compare(Tuple2<Polygon, HashSet<Polygon>> o1, Tuple2<Polygon, HashSet<Polygon>> o2) {
		Polygon p1 = o1._1;
		Polygon p2 = o2._1;
		Coordinate c1 = p1.getCoordinate();
		Coordinate c2 = p2.getCoordinate();
		if(c1.y > c2.y)
		{
			return 1;
		}
		else if(c1.y<c2.y)
		{
			return -1;
		}
		else
		{
			if(c1.x > c2.x)
			{
				return 1;
			}
			else if(c1.x < c2.x )
			{
				return -1;
			}
			else return 0;
		}
		
	}
}

/**
 * The Class CorrectnessChecker.
 */
public class CorrectnessChecker {
	/** The sc. */
    public static JavaSparkContext sc;
    
    /** The test polygon window set. */
    public static List<Polygon> testPolygonWindowSet;
    
    /** The test inside polygon set. */
    public static List<Polygon> testInsidePolygonSet;
    
    /** The test overlapped polygon set. */
    public static List<Polygon> testOverlappedPolygonSet;
    
    /** The test outside polygon set. */
    public static List<Polygon> testOutsidePolygonSet;
    
    /** The test inside line string set. */
    public static List<LineString> testInsideLineStringSet;
    
    /** The test overlapped line string set. */
    public static List<LineString> testOverlappedLineStringSet;
    
    /** The test outside line string set. */
    public static List<LineString> testOutsideLineStringSet;
    
    /** The test inside point set. */
    public static List<Point> testInsidePointSet;
    
    /** The test on boundary point set. */
    public static List<Point> testOnBoundaryPointSet;
    
    /** The test outside point set. */
    public static List<Point> testOutsidePointSet;
    
    /** The window object user data 1. */
    public static String windowObjectUserData1;
    
    /** The window object user data 2. */
    public static String windowObjectUserData2;
    
    /** The data object user data 1. */
    public static String dataObjectUserData1;
    
    /** The data object user data 2. */
    public static String dataObjectUserData2;
    
    /**
     * Once executed before all.
     */
    @BeforeClass
    public static void onceExecutedBeforeAll() {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName("CorrectnessChecker").setMaster("local[2]");
        sc = new JavaSparkContext(conf);
        GeometryFactory geometryFactory = new GeometryFactory();
        
        // Define the user data in window objects and data objects
        windowObjectUserData1 = "window1";
        windowObjectUserData2 = "window2";
        dataObjectUserData1 = "data1";
        dataObjectUserData2 = "data2";

        // Define the user data saved in window objects and data objects
        testPolygonWindowSet = new ArrayList<Polygon>();
        testInsidePolygonSet = new ArrayList<Polygon>();
        testOverlappedPolygonSet = new ArrayList<Polygon>();
        testOutsidePolygonSet = new ArrayList<Polygon>();
        
        testInsideLineStringSet = new ArrayList<LineString>();
        testOverlappedLineStringSet = new ArrayList<LineString>();
        testOutsideLineStringSet = new ArrayList<LineString>();
        
        testInsidePointSet = new ArrayList<Point>();
        testOnBoundaryPointSet = new ArrayList<Point>();
        testOutsidePointSet = new ArrayList<Point>();
        
        int testScale = 10000;
        // Generate all test data
        
        // Generate test windows. Each window is a 5 by 5 rectangle-style polygon.
        // All data is uniformly distributed in a testWindowNum by testWindowNum space.
    	double baseX = 0.0;
    	double baseY = 0.0;
        for(int i=0;i<testScale;i++)
        {
        	// Decide the base X and Y coordinate
        	baseX = (i%100.0)*10.0;
        	if(i%100==0)
        	{
        		baseY = (baseX/100.0)*10.0;
        	}
        	//Rectangle-style polygon window
        	Coordinate[] coordinates = new Coordinate[5];
        	coordinates[0] = new Coordinate(baseX,baseY);
        	coordinates[1] = new Coordinate(baseX+5.0,baseY);
        	coordinates[2] = new Coordinate(baseX+5.0,baseY+5.0);
        	coordinates[3] = new Coordinate(baseX,baseY+5.0);
        	coordinates[4] = coordinates[0];
        	
        	Polygon polygonWindow1 = geometryFactory.createPolygon(coordinates);
        	polygonWindow1.setUserData(windowObjectUserData1);
        	Polygon polygonWindow2 = geometryFactory.createPolygon(coordinates);
        	polygonWindow2.setUserData(windowObjectUserData2);
        	testPolygonWindowSet.add(polygonWindow1);
        	testPolygonWindowSet.add(polygonWindow2);

        	//Polygon data: inside
        	coordinates = new Coordinate[5];
        	coordinates[0] = new Coordinate(baseX+2.0,baseY+2.0);
        	coordinates[1] = new Coordinate(baseX+4.0,baseY+2.0);
        	coordinates[2] = new Coordinate(baseX+4.0,baseY+4.0);
        	coordinates[3] = new Coordinate(baseX+2.0,baseY+4.0);
        	coordinates[4] = coordinates[0];
        	
        	Polygon polygonData1 = geometryFactory.createPolygon(coordinates);
        	polygonData1.setUserData(dataObjectUserData1);
        	Polygon polygonData2 = geometryFactory.createPolygon(coordinates);
        	polygonData2.setUserData(dataObjectUserData2);
        	testInsidePolygonSet.add(polygonData1);
        	testInsidePolygonSet.add(polygonData2);
        	
        	//Polygon data: overlapped
        	coordinates = new Coordinate[5];
        	coordinates[0] = new Coordinate(baseX+3.0,baseY+3.0);
        	coordinates[1] = new Coordinate(baseX+6.0,baseY+3.0);
        	coordinates[2] = new Coordinate(baseX+6.0,baseY+6.0);
        	coordinates[3] = new Coordinate(baseX+3.0,baseY+6.0);
        	coordinates[4] = coordinates[0];
        	
        	polygonData1 = geometryFactory.createPolygon(coordinates);
        	polygonData1.setUserData(dataObjectUserData1);
        	polygonData2 = geometryFactory.createPolygon(coordinates);
        	polygonData2.setUserData(dataObjectUserData2);
        	testOverlappedPolygonSet.add(polygonData1);
        	testOverlappedPolygonSet.add(polygonData2);
        	
        	//Polygon data: outside
        	coordinates = new Coordinate[5];
        	coordinates[0] = new Coordinate(baseX+6.0,baseY+6.0);
        	coordinates[1] = new Coordinate(baseX+9.0,baseY+6.0);
        	coordinates[2] = new Coordinate(baseX+9.0,baseY+9.0);
        	coordinates[3] = new Coordinate(baseX+6.0,baseY+9.0);
        	coordinates[4] = coordinates[0];
        	
        	polygonData1 = geometryFactory.createPolygon(coordinates);
        	polygonData1.setUserData(dataObjectUserData1);
        	polygonData2 = geometryFactory.createPolygon(coordinates);
        	polygonData2.setUserData(dataObjectUserData2);
        	testOutsidePolygonSet.add(polygonData1);
        	testOutsidePolygonSet.add(polygonData2);
        	
        	//LineString data: inside
        	coordinates = new Coordinate[4];
        	coordinates[0] = new Coordinate(baseX+2.0,baseY+2.0);
        	coordinates[1] = new Coordinate(baseX+4.0,baseY+2.0);
        	coordinates[2] = new Coordinate(baseX+4.0,baseY+4.0);
        	coordinates[3] = new Coordinate(baseX+2.0,baseY+4.0);
        	
        	LineString lineStringData1 = geometryFactory.createLineString(coordinates);
        	lineStringData1.setUserData(dataObjectUserData1);
        	LineString lineStringData2 = geometryFactory.createLineString(coordinates);
        	lineStringData2.setUserData(dataObjectUserData2);
        	testInsideLineStringSet.add(lineStringData1);
        	testInsideLineStringSet.add(lineStringData2);
        	
        	//LineString data: overlapped
        	coordinates = new Coordinate[4];
        	coordinates[0] = new Coordinate(baseX+3.0,baseY+3.0);
        	coordinates[1] = new Coordinate(baseX+6.0,baseY+3.0);
        	coordinates[2] = new Coordinate(baseX+6.0,baseY+6.0);
        	coordinates[3] = new Coordinate(baseX+3.0,baseY+6.0);
        	
        	lineStringData1 = geometryFactory.createLineString(coordinates);
        	lineStringData1.setUserData(dataObjectUserData1);
        	lineStringData2 = geometryFactory.createLineString(coordinates);
        	lineStringData2.setUserData(dataObjectUserData2);
        	testOverlappedLineStringSet.add(lineStringData1);
        	testOverlappedLineStringSet.add(lineStringData2);
        	
        	//LineString data: outside
        	coordinates = new Coordinate[4];
        	coordinates[0] = new Coordinate(baseX+6.0,baseY+6.0);
        	coordinates[1] = new Coordinate(baseX+9.0,baseY+6.0);
        	coordinates[2] = new Coordinate(baseX+9.0,baseY+9.0);
        	coordinates[3] = new Coordinate(baseX+6.0,baseY+9.0);
        	
        	lineStringData1 = geometryFactory.createLineString(coordinates);
        	lineStringData1.setUserData(dataObjectUserData1);
        	lineStringData2 = geometryFactory.createLineString(coordinates);
        	lineStringData2.setUserData(dataObjectUserData2);
        	testOutsideLineStringSet.add(lineStringData1);
        	testOutsideLineStringSet.add(lineStringData2);
        	
        	
        	//Point data: inside
        	Coordinate coordinate = new Coordinate(baseX+2.5,baseY+2.5);
        	
        	Point point1 = geometryFactory.createPoint(coordinate);
        	point1.setUserData(dataObjectUserData1);
        	Point point2 = geometryFactory.createPoint(coordinate);
        	point2.setUserData(dataObjectUserData2);
        	testInsidePointSet.add(point1);
        	testInsidePointSet.add(point2);
        	
        	//Point data: on boundary
        	coordinate = new Coordinate(baseX+5.0,baseY+5.0);
        	
        	point1 = geometryFactory.createPoint(coordinate);
        	point1.setUserData(dataObjectUserData1);
        	point2 = geometryFactory.createPoint(coordinate);
        	point2.setUserData(dataObjectUserData2);
        	testOnBoundaryPointSet.add(point1);
        	testOnBoundaryPointSet.add(point2);
        	
        	//Point data: outside
        	coordinate = new Coordinate(baseX+6.0,baseY+6.0);
        	
        	point1 = geometryFactory.createPoint(coordinate);
        	point1.setUserData(dataObjectUserData1);
        	point2 = geometryFactory.createPoint(coordinate);
        	point2.setUserData(dataObjectUserData2);
        	testOutsidePointSet.add(point1);
        	testOutsidePointSet.add(point2);
        }
    }
    
    
    /**
     * Test inside point join correctness.
     *
     * @throws Exception the exception
     */
    @Test
    public void testInsidePointJoinCorrectness() throws Exception{
    	PolygonRDD windowRDD = new PolygonRDD(sc.parallelize(this.testPolygonWindowSet));
        PointRDD objectRDD = new PointRDD(sc.parallelize(this.testInsidePointSet));
        objectRDD.rawSpatialRDD.repartition(4);
        objectRDD.spatialPartitioning(GridType.RTREE);
        objectRDD.buildIndex(IndexType.RTREE,true);
        windowRDD.spatialPartitioning(objectRDD.grids);
        JavaPairRDD<Polygon,HashSet<Point>> resultRDD = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, true,false);
        List<Tuple2<Polygon,HashSet<Point>>> resultUnsort = resultRDD.collect();
        List<Tuple2<Polygon,HashSet<Point>>> result = new ArrayList<Tuple2<Polygon,HashSet<Point>>>(resultUnsort);
        Collections.sort(result,new PointByPolygonSortComparator());
        //System.out.println("Number of windows: " + result.size());
        assert result.size()==200;
        for(Tuple2<Polygon,HashSet<Point>> tuple:result)
        {
        	Polygon window = tuple._1;
        	HashSet<Point> objects = tuple._2;
        	String windowUserData = (String) window.getUserData();
    		assert windowUserData.equals(this.windowObjectUserData1) || windowUserData.equals(this.windowObjectUserData2);
        	//System.out.print(window+" "+windowUserData);
        	//System.out.print(" "+objects.size());
        	assert objects.size()==2;
        	for(Point object:objects)
        	{
        		
        		String objectUserData = (String) object.getUserData();
        		assert objectUserData.equals(this.dataObjectUserData1) || objectUserData.equals(this.dataObjectUserData2);
        		//System.out.print(" " + object+ " " + objectUserData);
        	}
        	//System.out.println();
        }
        
        JavaPairRDD<Polygon,HashSet<Point>> resultRDDNoIndex = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, false,false);
        List<Tuple2<Polygon,HashSet<Point>>> resultUnsortNoIndex = resultRDDNoIndex.collect();
        List<Tuple2<Polygon,HashSet<Point>>> resultNoIndex = new ArrayList<Tuple2<Polygon,HashSet<Point>>>(resultUnsortNoIndex);
        Collections.sort(resultNoIndex,new PointByPolygonSortComparator());
        //System.out.println("Number of windows: " + resultNoIndex.size());
        assert resultNoIndex.size()==200;
        for(Tuple2<Polygon,HashSet<Point>> tuple:resultNoIndex)
        {
        	Polygon window = tuple._1;
        	HashSet<Point> objects = tuple._2;
        	String windowUserData = (String) window.getUserData();
    		assert windowUserData.equals(this.windowObjectUserData1) || windowUserData.equals(this.windowObjectUserData2);
        	//System.out.print(window+" "+windowUserData);
        	//System.out.print(" "+objects.size());
        	assert objects.size()==2;
        	for(Point object:objects)
        	{
        		
        		String objectUserData = (String) object.getUserData();
        		assert objectUserData.equals(this.dataObjectUserData1) || objectUserData.equals(this.dataObjectUserData2);
        		//System.out.print(" " + object+ " " + objectUserData);
        	}
        	//System.out.println();
        }
    }
        
        /**
         * Test on boundary point join correctness.
         *
         * @throws Exception the exception
         */
        @Test
        public void testOnBoundaryPointJoinCorrectness() throws Exception{
        	PolygonRDD windowRDD = new PolygonRDD(sc.parallelize(this.testPolygonWindowSet));
            PointRDD objectRDD = new PointRDD(sc.parallelize(this.testOnBoundaryPointSet));
            objectRDD.rawSpatialRDD.repartition(4);
            objectRDD.spatialPartitioning(GridType.RTREE);
            objectRDD.buildIndex(IndexType.RTREE,true);
            windowRDD.spatialPartitioning(objectRDD.grids);
            JavaPairRDD<Polygon,HashSet<Point>> resultRDD = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, true,true);
            List<Tuple2<Polygon,HashSet<Point>>> resultUnsort = resultRDD.collect();
            List<Tuple2<Polygon,HashSet<Point>>> result = new ArrayList<Tuple2<Polygon,HashSet<Point>>>(resultUnsort);
            Collections.sort(result,new PointByPolygonSortComparator());
            //System.out.println("Number of windows: " + result.size());
            assert result.size()==200;
            for(Tuple2<Polygon,HashSet<Point>> tuple:result)
            {
            	Polygon window = tuple._1;
            	HashSet<Point> objects = tuple._2;
            	String windowUserData = (String) window.getUserData();
        		assert windowUserData.equals(this.windowObjectUserData1) || windowUserData.equals(this.windowObjectUserData2);
            	//System.out.print(window+" "+windowUserData);
            	//System.out.print(" "+objects.size());
            	assert objects.size()==2;
            	for(Point object:objects)
            	{
            		
            		String objectUserData = (String) object.getUserData();
            		assert objectUserData.equals(this.dataObjectUserData1) || objectUserData.equals(this.dataObjectUserData2);
            		//System.out.print(" " + object+ " " + objectUserData);
            	}
            	//System.out.println();
            }
            
            JavaPairRDD<Polygon,HashSet<Point>> resultRDDNoIndex = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, false,true);
            List<Tuple2<Polygon,HashSet<Point>>> resultUnsortNoIndex = resultRDDNoIndex.collect();
            List<Tuple2<Polygon,HashSet<Point>>> resultNoIndex = new ArrayList<Tuple2<Polygon,HashSet<Point>>>(resultUnsortNoIndex);
            Collections.sort(resultNoIndex,new PointByPolygonSortComparator());
            //System.out.println("Number of windows: " + resultNoIndex.size());
            assert resultNoIndex.size()==200;
            for(Tuple2<Polygon,HashSet<Point>> tuple:resultNoIndex)
            {
            	Polygon window = tuple._1;
            	HashSet<Point> objects = tuple._2;
            	String windowUserData = (String) window.getUserData();
        		assert windowUserData.equals(this.windowObjectUserData1) || windowUserData.equals(this.windowObjectUserData2);
            	//System.out.print(window+" "+windowUserData);
            	//System.out.print(" "+objects.size());
            	assert objects.size()==2;
            	for(Point object:objects)
            	{
            		
            		String objectUserData = (String) object.getUserData();
            		assert objectUserData.equals(this.dataObjectUserData1) || objectUserData.equals(this.dataObjectUserData2);
            		//System.out.print(" " + object+ " " + objectUserData);
            	}
            	//System.out.println();
            }
        }
        
        /**
         * Test outside point join correctness.
         *
         * @throws Exception the exception
         */
        @Test
        public void testOutsidePointJoinCorrectness() throws Exception{
        	PolygonRDD windowRDD = new PolygonRDD(sc.parallelize(this.testPolygonWindowSet));
            PointRDD objectRDD = new PointRDD(sc.parallelize(this.testOutsidePointSet));
            objectRDD.rawSpatialRDD.repartition(4);
            objectRDD.spatialPartitioning(GridType.RTREE);
            objectRDD.buildIndex(IndexType.RTREE,true);
            windowRDD.spatialPartitioning(objectRDD.grids);
            JavaPairRDD<Polygon,HashSet<Point>> resultRDD = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, true,false);
            List<Tuple2<Polygon,HashSet<Point>>> resultUnsort = resultRDD.collect();
            List<Tuple2<Polygon,HashSet<Point>>> result = new ArrayList<Tuple2<Polygon,HashSet<Point>>>(resultUnsort);
            Collections.sort(result,new PointByPolygonSortComparator());
            //System.out.println("Number of windows: " + result.size());
            assert result.size()==0;
            
            JavaPairRDD<Polygon,HashSet<Point>> resultRDDNoIndex = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, false,false);
            List<Tuple2<Polygon,HashSet<Point>>> resultUnsortNoIndex = resultRDDNoIndex.collect();
            List<Tuple2<Polygon,HashSet<Point>>> resultNoIndex = new ArrayList<Tuple2<Polygon,HashSet<Point>>>(resultUnsortNoIndex);
            Collections.sort(resultNoIndex,new PointByPolygonSortComparator());
            //System.out.println("Number of windows: " + resultNoIndex.size());
            assert resultNoIndex.size()==0;
        }
        
        /**
         * Test inside line string join correctness.
         *
         * @throws Exception the exception
         */
        @Test
        public void testInsideLineStringJoinCorrectness() throws Exception{
        	PolygonRDD windowRDD = new PolygonRDD(sc.parallelize(this.testPolygonWindowSet));
            LineStringRDD objectRDD = new LineStringRDD(sc.parallelize(this.testInsideLineStringSet));
            objectRDD.rawSpatialRDD.repartition(4);
            objectRDD.spatialPartitioning(GridType.RTREE);
            objectRDD.buildIndex(IndexType.RTREE,true);
            windowRDD.spatialPartitioning(objectRDD.grids);
            JavaPairRDD<Polygon,HashSet<LineString>> resultRDD = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, true,false);
            List<Tuple2<Polygon,HashSet<LineString>>> resultUnsort = resultRDD.collect();
            List<Tuple2<Polygon,HashSet<LineString>>> result = new ArrayList<Tuple2<Polygon,HashSet<LineString>>>(resultUnsort);
            //Collections.sort(result,new PolygonSortComparator());
            //System.out.println("Number of windows: " + result.size());
            assert result.size()==200;
            for(Tuple2<Polygon,HashSet<LineString>> tuple:result)
            {
            	Polygon window = tuple._1;
            	HashSet<LineString> objects = tuple._2;
            	String windowUserData = (String) window.getUserData();
        		assert windowUserData.equals(this.windowObjectUserData1) || windowUserData.equals(this.windowObjectUserData2);
            	//System.out.print(window+" "+windowUserData);
            	//System.out.print(" "+objects.size());
            	assert objects.size()==2;
            	for(LineString object:objects)
            	{
            		
            		String objectUserData = (String) object.getUserData();
            		assert objectUserData.equals(this.dataObjectUserData1) || objectUserData.equals(this.dataObjectUserData2);
            		//System.out.print(" " + object+ " " + objectUserData);
            	}
            	//System.out.println();
            }
            
            JavaPairRDD<Polygon,HashSet<LineString>> resultRDDNoIndex = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, false,false);
            List<Tuple2<Polygon,HashSet<LineString>>> resultUnsortNoIndex = resultRDDNoIndex.collect();
            List<Tuple2<Polygon,HashSet<LineString>>> resultNoIndex = new ArrayList<Tuple2<Polygon,HashSet<LineString>>>(resultUnsortNoIndex);
            //Collections.sort(resultNoIndex,new PolygonSortComparator());
            //System.out.println("Number of windows: " + resultNoIndex.size());
            assert resultNoIndex.size()==200;
            for(Tuple2<Polygon,HashSet<LineString>> tuple:resultNoIndex)
            {
            	Polygon window = tuple._1;
            	HashSet<LineString> objects = tuple._2;
            	String windowUserData = (String) window.getUserData();
        		assert windowUserData.equals(this.windowObjectUserData1) || windowUserData.equals(this.windowObjectUserData2);
            	//System.out.print(window+" "+windowUserData);
            	//System.out.print(" "+objects.size());
            	assert objects.size()==2;
            	for(LineString object:objects)
            	{
            		
            		String objectUserData = (String) object.getUserData();
            		assert objectUserData.equals(this.dataObjectUserData1) || objectUserData.equals(this.dataObjectUserData2);
            		//System.out.print(" " + object+ " " + objectUserData);
            	}
            	//System.out.println();
            }
        }
        
        /**
         * Test overlapped line string join correctness.
         *
         * @throws Exception the exception
         */
        @Test
        public void testOverlappedLineStringJoinCorrectness() throws Exception{
        	PolygonRDD windowRDD = new PolygonRDD(sc.parallelize(this.testPolygonWindowSet));
        	LineStringRDD objectRDD = new LineStringRDD(sc.parallelize(this.testOverlappedLineStringSet));
        	objectRDD.rawSpatialRDD.repartition(4);
        	objectRDD.spatialPartitioning(GridType.RTREE);
        	objectRDD.buildIndex(IndexType.RTREE,true);
        	windowRDD.spatialPartitioning(objectRDD.grids);
        	JavaPairRDD<Polygon,HashSet<LineString>> resultRDD = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, true,false);
        	List<Tuple2<Polygon,HashSet<LineString>>> resultUnsort = resultRDD.collect();
        	List<Tuple2<Polygon,HashSet<LineString>>> result = new ArrayList<Tuple2<Polygon,HashSet<LineString>>>(resultUnsort);
        	//Collections.sort(result,new PolygonSortComparator());
        	//System.out.println("Number of windows: " + result.size());
        	assert result.size()==0;
        	for(Tuple2<Polygon,HashSet<LineString>> tuple:result)
        	{
        		Polygon window = tuple._1;
        		HashSet<LineString> objects = tuple._2;
        		String windowUserData = (String) window.getUserData();
        		assert windowUserData.equals(this.windowObjectUserData1) || windowUserData.equals(this.windowObjectUserData2);
        		//System.out.print(window+" "+windowUserData);
        		//System.out.print(" "+objects.size());
        		assert objects.size()==0;
        		for(LineString object:objects)
        		{
        			
        			String objectUserData = (String) object.getUserData();
        			assert objectUserData.equals(this.dataObjectUserData1) || objectUserData.equals(this.dataObjectUserData2);
        			//System.out.print(" " + object+ " " + objectUserData);
                		}
                	//System.out.println();
                }
                
                JavaPairRDD<Polygon,HashSet<LineString>> resultRDDNoIndex = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, false,false);
                List<Tuple2<Polygon,HashSet<LineString>>> resultUnsortNoIndex = resultRDDNoIndex.collect();
                List<Tuple2<Polygon,HashSet<LineString>>> resultNoIndex = new ArrayList<Tuple2<Polygon,HashSet<LineString>>>(resultUnsortNoIndex);
                //Collections.sort(resultNoIndex,new PolygonSortComparator());
                //System.out.println("Number of windows: " + resultNoIndex.size());
                assert resultNoIndex.size()==0;
                for(Tuple2<Polygon,HashSet<LineString>> tuple:resultNoIndex)
                {
                	Polygon window = tuple._1;
                	HashSet<LineString> objects = tuple._2;
                	String windowUserData = (String) window.getUserData();
            		assert windowUserData.equals(this.windowObjectUserData1) || windowUserData.equals(this.windowObjectUserData2);
                	//System.out.print(window+" "+windowUserData);
                	//System.out.print(" "+objects.size());
                	assert objects.size()==2;
                	for(LineString object:objects)
                	{
                		
                		String objectUserData = (String) object.getUserData();
                		assert objectUserData.equals(this.dataObjectUserData1) || objectUserData.equals(this.dataObjectUserData2);
                		//System.out.print(" " + object+ " " + objectUserData);
                	}
                	//System.out.println();
                }
            }
            
            /**
             * Test outside line string join correctness.
             *
             * @throws Exception the exception
             */
            @Test
            public void testOutsideLineStringJoinCorrectness() throws Exception{
            	PolygonRDD windowRDD = new PolygonRDD(sc.parallelize(this.testPolygonWindowSet));
            	LineStringRDD objectRDD = new LineStringRDD(sc.parallelize(this.testOutsideLineStringSet));
                objectRDD.rawSpatialRDD.repartition(4);
                objectRDD.spatialPartitioning(GridType.RTREE);
                objectRDD.buildIndex(IndexType.RTREE,true);
                windowRDD.spatialPartitioning(objectRDD.grids);
                JavaPairRDD<Polygon,HashSet<LineString>> resultRDD = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, true,false);
                List<Tuple2<Polygon,HashSet<LineString>>> resultUnsort = resultRDD.collect();
                List<Tuple2<Polygon,HashSet<LineString>>> result = new ArrayList<Tuple2<Polygon,HashSet<LineString>>>(resultUnsort);
                //Collections.sort(result,new PolygonSortComparator());
                //System.out.println("Number of windows: " + result.size());
                assert result.size()==0;
                
                JavaPairRDD<Polygon,HashSet<LineString>> resultRDDNoIndex = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, false,false);
                List<Tuple2<Polygon,HashSet<LineString>>> resultUnsortNoIndex = resultRDDNoIndex.collect();
                List<Tuple2<Polygon,HashSet<LineString>>> resultNoIndex = new ArrayList<Tuple2<Polygon,HashSet<LineString>>>(resultUnsortNoIndex);
                //Collections.sort(resultNoIndex,new PolygonSortComparator());
                //System.out.println("Number of windows: " + resultNoIndex.size());
                assert resultNoIndex.size()==0;
            }
        
            
            /**
             * Test inside polygon join correctness.
             *
             * @throws Exception the exception
             */
            @Test
            public void testInsidePolygonJoinCorrectness() throws Exception{
            	PolygonRDD windowRDD = new PolygonRDD(sc.parallelize(this.testPolygonWindowSet));
            	PolygonRDD objectRDD = new PolygonRDD(sc.parallelize(this.testInsidePolygonSet));
                objectRDD.rawSpatialRDD.repartition(4);
                objectRDD.spatialPartitioning(GridType.RTREE);
                objectRDD.buildIndex(IndexType.RTREE,true);
                windowRDD.spatialPartitioning(objectRDD.grids);
                JavaPairRDD<Polygon,HashSet<Polygon>> resultRDD = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, true,false);
                List<Tuple2<Polygon,HashSet<Polygon>>> resultUnsort = resultRDD.collect();
                List<Tuple2<Polygon,HashSet<Polygon>>> result = new ArrayList<Tuple2<Polygon,HashSet<Polygon>>>(resultUnsort);
                //Collections.sort(result,new PolygonSortComparator());
                //System.out.println("Number of windows: " + result.size());
                assert result.size()==200;
                for(Tuple2<Polygon,HashSet<Polygon>> tuple:result)
                {
                	Polygon window = tuple._1;
                	HashSet<Polygon> objects = tuple._2;
                	String windowUserData = (String) window.getUserData();
            		assert windowUserData.equals(this.windowObjectUserData1) || windowUserData.equals(this.windowObjectUserData2);
                	//System.out.print(window+" "+windowUserData);
                	//System.out.print(" "+objects.size());
                	assert objects.size()==2;
                	for(Polygon object:objects)
                	{
                		
                		String objectUserData = (String) object.getUserData();
                		assert objectUserData.equals(this.dataObjectUserData1) || objectUserData.equals(this.dataObjectUserData2);
                		//System.out.print(" " + object+ " " + objectUserData);
                	}
                	//System.out.println();
                }
                
                JavaPairRDD<Polygon,HashSet<Polygon>> resultRDDNoIndex = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, false,false);
                List<Tuple2<Polygon,HashSet<Polygon>>> resultUnsortNoIndex = resultRDDNoIndex.collect();
                List<Tuple2<Polygon,HashSet<Polygon>>> resultNoIndex = new ArrayList<Tuple2<Polygon,HashSet<Polygon>>>(resultUnsortNoIndex);
                //Collections.sort(resultNoIndex,new PolygonSortComparator());
                //System.out.println("Number of windows: " + resultNoIndex.size());
                assert resultNoIndex.size()==200;
                for(Tuple2<Polygon,HashSet<Polygon>> tuple:resultNoIndex)
                {
                	Polygon window = tuple._1;
                	HashSet<Polygon> objects = tuple._2;
                	String windowUserData = (String) window.getUserData();
            		assert windowUserData.equals(this.windowObjectUserData1) || windowUserData.equals(this.windowObjectUserData2);
                	//System.out.print(window+" "+windowUserData);
                	//System.out.print(" "+objects.size());
                	assert objects.size()==2;
                	for(Polygon object:objects)
                	{
                		
                		String objectUserData = (String) object.getUserData();
                		assert objectUserData.equals(this.dataObjectUserData1) || objectUserData.equals(this.dataObjectUserData2);
                		//System.out.print(" " + object+ " " + objectUserData);
                	}
                	//System.out.println();
                }
            }
            
            /**
             * Test overlapped polygon join correctness.
             *
             * @throws Exception the exception
             */
            @Test
            public void testOverlappedPolygonJoinCorrectness() throws Exception{
            	PolygonRDD windowRDD = new PolygonRDD(sc.parallelize(this.testPolygonWindowSet));
            	PolygonRDD objectRDD = new PolygonRDD(sc.parallelize(this.testOverlappedPolygonSet));
            	objectRDD.rawSpatialRDD.repartition(4);
            	objectRDD.spatialPartitioning(GridType.RTREE);
            	objectRDD.buildIndex(IndexType.RTREE,true);
            	windowRDD.spatialPartitioning(objectRDD.grids);
            	JavaPairRDD<Polygon,HashSet<Polygon>> resultRDD = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, true,true);
            	List<Tuple2<Polygon,HashSet<Polygon>>> resultUnsort = resultRDD.collect();
            	List<Tuple2<Polygon,HashSet<Polygon>>> result = new ArrayList<Tuple2<Polygon,HashSet<Polygon>>>(resultUnsort);
            	//Collections.sort(result,new PolygonSortComparator());
            	//System.out.println("Number of windows: " + result.size());
            	assert result.size()==200;
            	for(Tuple2<Polygon,HashSet<Polygon>> tuple:result)
            	{
            		Polygon window = tuple._1;
            		HashSet<Polygon> objects = tuple._2;
            		String windowUserData = (String) window.getUserData();
            		assert windowUserData.equals(this.windowObjectUserData1) || windowUserData.equals(this.windowObjectUserData2);
            		//System.out.print(window+" "+windowUserData);
            		//System.out.print(" "+objects.size());
            		assert objects.size()==2;
            		for(Polygon object:objects)
            		{

            			String objectUserData = (String) object.getUserData();
            			assert objectUserData.equals(this.dataObjectUserData1) || objectUserData.equals(this.dataObjectUserData2);
            			//System.out.print(" " + object+ " " + objectUserData);
            		}
            		//System.out.println();
            	}

            	JavaPairRDD<Polygon,HashSet<Polygon>> resultRDDNoIndex = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, false,true);
            	List<Tuple2<Polygon,HashSet<Polygon>>> resultUnsortNoIndex = resultRDDNoIndex.collect();
            	List<Tuple2<Polygon,HashSet<Polygon>>> resultNoIndex = new ArrayList<Tuple2<Polygon,HashSet<Polygon>>>(resultUnsortNoIndex);
            	//Collections.sort(resultNoIndex,new PolygonSortComparator());
            	//System.out.println("Number of windows: " + resultNoIndex.size());
            	assert resultNoIndex.size()==200;
            	for(Tuple2<Polygon,HashSet<Polygon>> tuple:resultNoIndex)
            	{
            		Polygon window = tuple._1;
            		HashSet<Polygon> objects = tuple._2;
            		String windowUserData = (String) window.getUserData();
            		assert windowUserData.equals(this.windowObjectUserData1) || windowUserData.equals(this.windowObjectUserData2);
            		//System.out.print(window+" "+windowUserData);
            		//System.out.print(" "+objects.size());
            		assert objects.size()==2;
            		for(Polygon object:objects)
            		{

            			String objectUserData = (String) object.getUserData();
            			assert objectUserData.equals(this.dataObjectUserData1) || objectUserData.equals(this.dataObjectUserData2);
            			//System.out.print(" " + object+ " " + objectUserData);
            		}
            		//System.out.println();
            	}
            }

            /**
             * Test outside polygon join correctness.
             *
             * @throws Exception the exception
             */
            @Test
            public void testOutsidePolygonJoinCorrectness() throws Exception{
            	PolygonRDD windowRDD = new PolygonRDD(sc.parallelize(this.testPolygonWindowSet));
            	PolygonRDD objectRDD = new PolygonRDD(sc.parallelize(this.testOutsidePolygonSet));
            	objectRDD.rawSpatialRDD.repartition(4);
            	objectRDD.spatialPartitioning(GridType.RTREE);
            	objectRDD.buildIndex(IndexType.RTREE,true);
            	windowRDD.spatialPartitioning(objectRDD.grids);
            	JavaPairRDD<Polygon,HashSet<Polygon>> resultRDD = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, true,false);
            	List<Tuple2<Polygon,HashSet<Polygon>>> resultUnsort = resultRDD.collect();
            	List<Tuple2<Polygon,HashSet<Polygon>>> result = new ArrayList<Tuple2<Polygon,HashSet<Polygon>>>(resultUnsort);
            	//Collections.sort(result,new PolygonSortComparator());
            	//System.out.println("Number of windows: " + result.size());
            	assert result.size()==0;

            	JavaPairRDD<Polygon,HashSet<Polygon>> resultRDDNoIndex = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, false,false);
            	List<Tuple2<Polygon,HashSet<Polygon>>> resultUnsortNoIndex = resultRDDNoIndex.collect();
            	List<Tuple2<Polygon,HashSet<Polygon>>> resultNoIndex = new ArrayList<Tuple2<Polygon,HashSet<Polygon>>>(resultUnsortNoIndex);
            	//Collections.sort(resultNoIndex,new PolygonSortComparator());
            	//System.out.println("Number of windows: " + resultNoIndex.size());
            	assert resultNoIndex.size()==0;
            } 

            /**
             * Test inside polygon distance join correctness.
             *
             * @throws Exception the exception
             */
            @Test
            public void testInsidePolygonDistanceJoinCorrectness() throws Exception{
            	PolygonRDD centerGeometryRDD = new PolygonRDD(sc.parallelize(this.testPolygonWindowSet));
            	CircleRDD windowRDD = new CircleRDD(centerGeometryRDD,0.1);
            	PolygonRDD objectRDD = new PolygonRDD(sc.parallelize(this.testInsidePolygonSet));
            	objectRDD.rawSpatialRDD.repartition(4);
            	objectRDD.spatialPartitioning(GridType.RTREE);
            	objectRDD.buildIndex(IndexType.RTREE,true);
            	windowRDD.spatialPartitioning(objectRDD.grids);
            	JavaPairRDD<Polygon,HashSet<Polygon>> resultRDD = JoinQuery.DistanceJoinQuery(objectRDD, windowRDD, true,false);
            	List<Tuple2<Polygon,HashSet<Polygon>>> resultUnsort = resultRDD.collect();
            	List<Tuple2<Polygon,HashSet<Polygon>>> result = new ArrayList<Tuple2<Polygon,HashSet<Polygon>>>(resultUnsort);
            	Collections.sort(result,new PolygonByPolygonSortComparator());
            	//System.out.println("Number of windows: " + result.size());
            	assert result.size()==200;
            	for(Tuple2<Polygon,HashSet<Polygon>> tuple:result)
            	{
            		Polygon window = tuple._1;
            		HashSet<Polygon> objects = tuple._2;
            		String windowUserData = (String) window.getUserData();
            		assert windowUserData.equals(this.windowObjectUserData1) || windowUserData.equals(this.windowObjectUserData2);
            		//System.out.print(window+" "+windowUserData);
            		//System.out.print(" "+objects.size());
            		assert objects.size()==2;
            		for(Polygon object:objects)
            		{

            			String objectUserData = (String) object.getUserData();
            			assert objectUserData.equals(this.dataObjectUserData1) || objectUserData.equals(this.dataObjectUserData2);
            			//System.out.print(" " + object+ " " + objectUserData);
            		}
            		//System.out.println();
            	}

            	JavaPairRDD<Polygon,HashSet<Polygon>> resultRDDNoIndex = JoinQuery.DistanceJoinQuery(objectRDD, windowRDD, false,false);
            	List<Tuple2<Polygon,HashSet<Polygon>>> resultUnsortNoIndex = resultRDDNoIndex.collect();
            	List<Tuple2<Polygon,HashSet<Polygon>>> resultNoIndex = new ArrayList<Tuple2<Polygon,HashSet<Polygon>>>(resultUnsortNoIndex);
            	Collections.sort(resultNoIndex,new PolygonByPolygonSortComparator());
            	//System.out.println("Number of windows: " + resultNoIndex.size());
            	assert resultNoIndex.size()==200;
            	for(Tuple2<Polygon,HashSet<Polygon>> tuple:resultNoIndex)
            	{
            		Polygon window = tuple._1;
            		HashSet<Polygon> objects = tuple._2;
            		String windowUserData = (String) window.getUserData();
            		assert windowUserData.equals(this.windowObjectUserData1) || windowUserData.equals(this.windowObjectUserData2);
            		//System.out.print(window+" "+windowUserData);
            		//System.out.print(" "+objects.size());
            		assert objects.size()==2;
            		for(Polygon object:objects)
            		{

            			String objectUserData = (String) object.getUserData();
            			assert objectUserData.equals(this.dataObjectUserData1) || objectUserData.equals(this.dataObjectUserData2);
            			//System.out.print(" " + object+ " " + objectUserData);
            		}
            		//System.out.println();
            	}
            }
            
            
            /**
             * Test overlapped polygon distance join correctness.
             *
             * @throws Exception the exception
             */
            @Test
            public void testOverlappedPolygonDistanceJoinCorrectness() throws Exception{
            	PolygonRDD centerGeometryRDD = new PolygonRDD(sc.parallelize(this.testPolygonWindowSet));
            	CircleRDD windowRDD = new CircleRDD(centerGeometryRDD,0.1);
            	PolygonRDD objectRDD = new PolygonRDD(sc.parallelize(this.testOverlappedPolygonSet));
            	objectRDD.rawSpatialRDD.repartition(4);
            	objectRDD.spatialPartitioning(GridType.RTREE);
            	objectRDD.buildIndex(IndexType.RTREE,true);
            	windowRDD.spatialPartitioning(objectRDD.grids);
            	JavaPairRDD<Polygon,HashSet<Polygon>> resultRDD = JoinQuery.DistanceJoinQuery(objectRDD, windowRDD, true,true);
            	List<Tuple2<Polygon,HashSet<Polygon>>> resultUnsort = resultRDD.collect();
            	List<Tuple2<Polygon,HashSet<Polygon>>> result = new ArrayList<Tuple2<Polygon,HashSet<Polygon>>>(resultUnsort);
            	Collections.sort(result,new PolygonByPolygonSortComparator());
            	//System.out.println("Number of windows: " + result.size());
            	assert result.size()==200;
            	for(Tuple2<Polygon,HashSet<Polygon>> tuple:result)
            	{
            		Polygon window = tuple._1;
            		HashSet<Polygon> objects = tuple._2;
            		String windowUserData = (String) window.getUserData();
            		assert windowUserData.equals(this.windowObjectUserData1) || windowUserData.equals(this.windowObjectUserData2);
            		//System.out.print(window+" "+windowUserData);
            		//System.out.print(" "+objects.size());
            		assert objects.size()==2;
            		for(Polygon object:objects)
            		{

            			String objectUserData = (String) object.getUserData();
            			assert objectUserData.equals(this.dataObjectUserData1) || objectUserData.equals(this.dataObjectUserData2);
            			//System.out.print(" " + object+ " " + objectUserData);
            		}
            		//System.out.println();
            	}

            	JavaPairRDD<Polygon,HashSet<Polygon>> resultRDDNoIndex = JoinQuery.DistanceJoinQuery(objectRDD, windowRDD, false,true);
            	List<Tuple2<Polygon,HashSet<Polygon>>> resultUnsortNoIndex = resultRDDNoIndex.collect();
            	List<Tuple2<Polygon,HashSet<Polygon>>> resultNoIndex = new ArrayList<Tuple2<Polygon,HashSet<Polygon>>>(resultUnsortNoIndex);
            	Collections.sort(resultNoIndex,new PolygonByPolygonSortComparator());
            	//System.out.println("Number of windows: " + resultNoIndex.size());
            	assert resultNoIndex.size()==200;
            	for(Tuple2<Polygon,HashSet<Polygon>> tuple:resultNoIndex)
            	{
            		Polygon window = tuple._1;
            		HashSet<Polygon> objects = tuple._2;
            		String windowUserData = (String) window.getUserData();
            		assert windowUserData.equals(this.windowObjectUserData1) || windowUserData.equals(this.windowObjectUserData2);
            		//System.out.print(window+" "+windowUserData);
            		//System.out.print(" "+objects.size());
            		assert objects.size()==2;
            		for(Polygon object:objects)
            		{

            			String objectUserData = (String) object.getUserData();
            			assert objectUserData.equals(this.dataObjectUserData1) || objectUserData.equals(this.dataObjectUserData2);
            			//System.out.print(" " + object+ " " + objectUserData);
            		}
            		//System.out.println();
            	}
            }

            /**
             * Test outside polygon distance join correctness.
             *
             * @throws Exception the exception
             */
            @Test
            public void testOutsidePolygonDistanceJoinCorrectness() throws Exception{
            	PolygonRDD centerGeometryRDD = new PolygonRDD(sc.parallelize(this.testPolygonWindowSet));
            	CircleRDD windowRDD = new CircleRDD(centerGeometryRDD,0.1);
            	PolygonRDD objectRDD = new PolygonRDD(sc.parallelize(this.testOutsidePolygonSet));
            	objectRDD.rawSpatialRDD.repartition(4);
            	objectRDD.spatialPartitioning(GridType.RTREE);
            	objectRDD.buildIndex(IndexType.RTREE,true);
            	windowRDD.spatialPartitioning(objectRDD.grids);
            	JavaPairRDD<Polygon,HashSet<Polygon>> resultRDD = JoinQuery.DistanceJoinQuery(objectRDD, windowRDD, true,true);
            	List<Tuple2<Polygon,HashSet<Polygon>>> resultUnsort = resultRDD.collect();
            	List<Tuple2<Polygon,HashSet<Polygon>>> result = new ArrayList<Tuple2<Polygon,HashSet<Polygon>>>(resultUnsort);
            	Collections.sort(result,new PolygonByPolygonSortComparator());
            	//System.out.println("Number of windows: " + result.size());
            	assert result.size()==0;
            	
            	JavaPairRDD<Polygon,HashSet<Polygon>> resultRDDNoIndex = JoinQuery.DistanceJoinQuery(objectRDD, windowRDD, false,true);
            	List<Tuple2<Polygon,HashSet<Polygon>>> resultUnsortNoIndex = resultRDDNoIndex.collect();
            	List<Tuple2<Polygon,HashSet<Polygon>>> resultNoIndex = new ArrayList<Tuple2<Polygon,HashSet<Polygon>>>(resultUnsortNoIndex);
            	Collections.sort(resultNoIndex,new PolygonByPolygonSortComparator());
            	//System.out.println("Number of windows: " + resultNoIndex.size());
            	assert resultNoIndex.size()==0;
            }

        /**
         * Tear down.
         */
        @AfterClass
        public static void TearDown() {
            sc.stop();
        }
        
}
