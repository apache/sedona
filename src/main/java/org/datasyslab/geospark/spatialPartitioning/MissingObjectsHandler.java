package org.datasyslab.geospark.spatialPartitioning;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.datasyslab.geospark.geometryObjects.EnvelopeWithGrid;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;

import scala.Tuple2;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.strtree.STRtree;

public class MissingObjectsHandler implements Serializable{

	public MissingObjectsHandler()
	{
		
	}
	public JavaPairRDD<Envelope,HashSet<Point>> handleMissingObjects(JavaSparkContext sc,PointRDD objectRDD,RectangleRDD queryWindowRDD)
	{
    	//This part is used to separate missed objects and covered objects by sample data based spatial partitioning.
    	final int gridSize=objectRDD.grids.size();
    	JavaPairRDD<Integer,Point> missedObjectRDDWithID=objectRDD.gridPointRDD.filter(new Function<Tuple2<Integer,Point>,Boolean>(){
			@Override
			public Boolean call(Tuple2<Integer, Point> object) throws Exception {
				// TODO Auto-generated method stub
				if(object._1()>=gridSize)
				{
					return true;
				}
				return false;
			}});
    	missedObjectRDDWithID.partitionBy(new SpatialPartitioner(gridSize));
    	JavaRDD<Point> missedObjectRDD=missedObjectRDDWithID.map(new Function<Tuple2<Integer,Point>, Point>()
    			{

					@Override
					public Point call(Tuple2<Integer, Point> object)
							throws Exception {
						return object._2();
					}
    		
    			});
    	JavaPairRDD<Integer,Envelope> missedWindowRDDWithID=queryWindowRDD.gridRectangleRDD.filter(new Function<Tuple2<Integer,Envelope>,Boolean>(){
			@Override
			public Boolean call(Tuple2<Integer, Envelope> object) throws Exception {
				// TODO Auto-generated method stub
				if(object._1()>=gridSize)
				{
					return true;
				}
				return false;
			}});
    	
    	JavaRDD<Envelope> missedWindowRDD=missedWindowRDDWithID.map(new Function<Tuple2<Integer,Envelope>, Envelope>()
    			{

					@Override
					public Envelope call(Tuple2<Integer, Envelope> object)
							throws Exception {
						return object._2();
					}
    		
    			});
    	List<Envelope> collectWindow=missedWindowRDD.collect();
    	final Broadcast<List<Envelope>> missedWindows = sc.broadcast(collectWindow);
    	JavaPairRDD<Envelope,Point> missedResult=missedObjectRDD.flatMapToPair(new PairFlatMapFunction<Point,Envelope,Point>()
    			{

					@Override
					public Iterable<Tuple2<Envelope, Point>> call(Point t)
							throws Exception {
						Iterator<Envelope> windowIterator=missedWindows.getValue().iterator();
						List<Tuple2<Envelope, Point>> result=new ArrayList<Tuple2<Envelope, Point>>();
						while(windowIterator.hasNext())
						{
							Envelope window=windowIterator.next();
							if(window.contains(t.getCoordinate()))
							{
								result.add(new Tuple2<Envelope,Point>(window,t));
							}
						}
						return result;
					}
    			});
    	JavaPairRDD<Envelope,Iterable<Point>> groupbyMissedResult=missedResult.groupByKey();
    	JavaPairRDD<Envelope,HashSet<Point>> mapMissedResult=groupbyMissedResult.mapToPair(new PairFunction<Tuple2<Envelope,Iterable<Point>>,Envelope,HashSet<Point>>()
    			{

					@Override
					public Tuple2<Envelope, HashSet<Point>> call(
							Tuple2<Envelope, Iterable<Point>> t)
							throws Exception {
						Iterator<Point> objectIterator=t._2().iterator();
						HashSet<Point> result=new HashSet<Point>();
						while(objectIterator.hasNext())
						{
							result.add(objectIterator.next());
						}
						return new Tuple2<Envelope,HashSet<Point>>(t._1(),result);
					}
    		
    			});
    	return mapMissedResult;
	}
	public JavaPairRDD<Envelope,HashSet<Point>> handleMissingObjectsUsingIndex(JavaSparkContext sc,PointRDD objectRDD,RectangleRDD queryWindowRDD)
	{
    	//This part is used to separate missed objects and covered objects by sample data based spatial partitioning.
    	final int gridSize=objectRDD.grids.size();
    	JavaPairRDD<Integer,STRtree> missedObjectRDDWithID=objectRDD.indexedRDD.filter(new Function<Tuple2<Integer,STRtree>,Boolean>(){
			@Override
			public Boolean call(Tuple2<Integer, STRtree> object) throws Exception {
				// TODO Auto-generated method stub
				if(object._1()>=gridSize)
				{
					return true;
				}
				return false;
			}});
    	missedObjectRDDWithID.partitionBy(new SpatialPartitioner(gridSize));
    	JavaRDD<STRtree> missedObjectRDD=missedObjectRDDWithID.map(new Function<Tuple2<Integer,STRtree>, STRtree>()
    			{

					@Override
					public STRtree call(Tuple2<Integer, STRtree> object)
							throws Exception {
						return object._2();
					}
    		
    			});
    	JavaPairRDD<Integer,Envelope> missedWindowRDDWithID=queryWindowRDD.gridRectangleRDD.filter(new Function<Tuple2<Integer,Envelope>,Boolean>(){
			@Override
			public Boolean call(Tuple2<Integer, Envelope> object) throws Exception {
				// TODO Auto-generated method stub
				if(object._1()>=gridSize)
				{
					return true;
				}
				return false;
			}});
    	
    	JavaRDD<Envelope> missedWindowRDD=missedWindowRDDWithID.map(new Function<Tuple2<Integer,Envelope>, Envelope>()
    			{

					@Override
					public Envelope call(Tuple2<Integer, Envelope> object)
							throws Exception {
						return object._2();
					}
    		
    			});
    	List<Envelope> collectWindow=missedWindowRDD.collect();
    	final Broadcast<List<Envelope>> missedWindows = sc.broadcast(collectWindow);
    	JavaPairRDD<Envelope,Point> missedResult=missedObjectRDD.flatMapToPair(new PairFlatMapFunction<STRtree,Envelope,Point>()
    			{

					@Override
					public Iterable<Tuple2<Envelope, Point>> call(STRtree t)
							throws Exception {
						Iterator<Envelope> windowIterator=missedWindows.getValue().iterator();
						List<Tuple2<Envelope, Point>> result=new ArrayList<Tuple2<Envelope, Point>>();
						while(windowIterator.hasNext())
						{
							Envelope window=windowIterator.next();
							List<Point> containedResult=t.query(window);
							Iterator<Point> objectIterator= containedResult.iterator();
							while(objectIterator.hasNext())
							{
								result.add(new Tuple2<Envelope,Point>(window,objectIterator.next()));
							}	
						}
						return result;
					}
    			});
    	JavaPairRDD<Envelope,Iterable<Point>> groupbyMissedResult=missedResult.groupByKey();
    	JavaPairRDD<Envelope,HashSet<Point>> mapMissedResult=groupbyMissedResult.mapToPair(new PairFunction<Tuple2<Envelope,Iterable<Point>>,Envelope,HashSet<Point>>()
    			{

					@Override
					public Tuple2<Envelope, HashSet<Point>> call(
							Tuple2<Envelope, Iterable<Point>> t)
							throws Exception {
						Iterator<Point> objectIterator=t._2().iterator();
						HashSet<Point> result=new HashSet<Point>();
						while(objectIterator.hasNext())
						{
							result.add(objectIterator.next());
						}
						return new Tuple2<Envelope,HashSet<Point>>(t._1(),result);
					}
    		
    			});
    	return mapMissedResult;
	}
	public JavaPairRDD<Envelope,HashSet<Envelope>> handleMissingObjects(JavaSparkContext sc,RectangleRDD objectRDD,RectangleRDD queryWindowRDD)
	{
    	//This part is used to separate missed objects and covered objects by sample data based spatial partitioning.
    	final int gridSize=objectRDD.grids.size();
    	JavaPairRDD<Integer,Envelope> missedObjectRDDWithID=objectRDD.gridRectangleRDD.filter(new Function<Tuple2<Integer,Envelope>,Boolean>(){
			@Override
			public Boolean call(Tuple2<Integer, Envelope> object) throws Exception {
				// TODO Auto-generated method stub
				if(object._1()>=gridSize)
				{
					return true;
				}
				return false;
			}});
    	missedObjectRDDWithID.partitionBy(new SpatialPartitioner(gridSize));
    	JavaRDD<Envelope> missedObjectRDD=missedObjectRDDWithID.map(new Function<Tuple2<Integer,Envelope>, Envelope>()
    			{

					@Override
					public Envelope call(Tuple2<Integer, Envelope> object)
							throws Exception {
						return object._2();
					}
    		
    			});
    	JavaPairRDD<Integer,Envelope> missedWindowRDDWithID=queryWindowRDD.gridRectangleRDD.filter(new Function<Tuple2<Integer,Envelope>,Boolean>(){
			@Override
			public Boolean call(Tuple2<Integer, Envelope> object) throws Exception {
				// TODO Auto-generated method stub
				if(object._1()>=gridSize)
				{
					return true;
				}
				return false;
			}});
    	JavaRDD<Envelope> missedWindowRDD=missedWindowRDDWithID.map(new Function<Tuple2<Integer,Envelope>, Envelope>()
    			{

					@Override
					public Envelope call(Tuple2<Integer, Envelope> object)
							throws Exception {
						return object._2();
					}
    		
    			});
    	List<Envelope> collectWindow=missedWindowRDD.collect();
    	final Broadcast<List<Envelope>> missedWindows = sc.broadcast(collectWindow);
    	JavaPairRDD<Envelope,Envelope> missedResult=missedObjectRDD.flatMapToPair(new PairFlatMapFunction<Envelope,Envelope,Envelope>()
    			{

					@Override
					public Iterable<Tuple2<Envelope, Envelope>> call(Envelope t)
							throws Exception {
						Iterator<Envelope> windowIterator=missedWindows.getValue().iterator();
						List<Tuple2<Envelope, Envelope>> result=new ArrayList<Tuple2<Envelope, Envelope>>();
						while(windowIterator.hasNext())
						{
							Envelope window=windowIterator.next();
							if(window.contains(t))
							{
								result.add(new Tuple2<Envelope,Envelope>(window,t));
							}
						}
						return result;
					}
    			});
    	JavaPairRDD<Envelope,Iterable<Envelope>> groupbyMissedResult=missedResult.groupByKey();
    	JavaPairRDD<Envelope,HashSet<Envelope>> mapMissedResult=groupbyMissedResult.mapToPair(new PairFunction<Tuple2<Envelope,Iterable<Envelope>>,Envelope,HashSet<Envelope>>()
    			{

					@Override
					public Tuple2<Envelope, HashSet<Envelope>> call(
							Tuple2<Envelope, Iterable<Envelope>> t)
							throws Exception {
						Iterator<Envelope> objectIterator=t._2().iterator();
						HashSet<Envelope> result=new HashSet<Envelope>();
						while(objectIterator.hasNext())
						{
							result.add(objectIterator.next());
						}
						return new Tuple2<Envelope,HashSet<Envelope>>(t._1(),result);
					}
    		
    			});
    	return mapMissedResult;
	}
	public JavaPairRDD<Envelope,HashSet<Envelope>> handleMissingObjectsUsingIndex(JavaSparkContext sc, RectangleRDD objectRDD,RectangleRDD queryWindowRDD)
	{
    	//This part is used to separate missed objects and covered objects by sample data based spatial partitioning.
    	final int gridSize=objectRDD.grids.size();
    	JavaPairRDD<Integer,STRtree> missedObjectRDDWithID=objectRDD.indexedRDD.filter(new Function<Tuple2<Integer,STRtree>,Boolean>(){
			@Override
			public Boolean call(Tuple2<Integer, STRtree> object) throws Exception {
				// TODO Auto-generated method stub
				if(object._1()>=gridSize)
				{
					return true;
				}
				return false;
			}});
    	missedObjectRDDWithID.partitionBy(new SpatialPartitioner(gridSize));
    	JavaRDD<STRtree> missedObjectRDD=missedObjectRDDWithID.map(new Function<Tuple2<Integer,STRtree>, STRtree>()
    			{

					@Override
					public STRtree call(Tuple2<Integer, STRtree> object)
							throws Exception {
						return object._2();
					}
    		
    			});
    	JavaPairRDD<Integer,Envelope> missedWindowRDDWithID=queryWindowRDD.gridRectangleRDD.filter(new Function<Tuple2<Integer,Envelope>,Boolean>(){
			@Override
			public Boolean call(Tuple2<Integer, Envelope> object) throws Exception {
				// TODO Auto-generated method stub
				if(object._1()>=gridSize)
				{
					return true;
				}
				return false;
			}});
    	JavaRDD<Envelope> missedWindowRDD=missedWindowRDDWithID.map(new Function<Tuple2<Integer,Envelope>, Envelope>()
    			{

					@Override
					public Envelope call(Tuple2<Integer, Envelope> object)
							throws Exception {
						return object._2();
					}
    		
    			});
    	List<Envelope> collectWindow=missedWindowRDD.collect();
    	final Broadcast<List<Envelope>> missedWindows = sc.broadcast(collectWindow);
    	JavaPairRDD<Envelope,Envelope> missedResult=missedObjectRDD.flatMapToPair(new PairFlatMapFunction<STRtree,Envelope,Envelope>()
    			{

					@Override
					public Iterable<Tuple2<Envelope, Envelope>> call(STRtree t)
							throws Exception {
						Iterator<Envelope> windowIterator=missedWindows.getValue().iterator();
						List<Tuple2<Envelope, Envelope>> result=new ArrayList<Tuple2<Envelope, Envelope>>();
						while(windowIterator.hasNext())
						{
							Envelope window=windowIterator.next();
							List<Envelope> containedResult=t.query(window);
							Iterator<Envelope> objectIterator= containedResult.iterator();
							while(objectIterator.hasNext())
							{
								result.add(new Tuple2<Envelope,Envelope>(window,objectIterator.next()));
							}	
						}
						return result;
					}
    			});
    	JavaPairRDD<Envelope,Iterable<Envelope>> groupbyMissedResult=missedResult.groupByKey();
    	JavaPairRDD<Envelope,HashSet<Envelope>> mapMissedResult=groupbyMissedResult.mapToPair(new PairFunction<Tuple2<Envelope,Iterable<Envelope>>,Envelope,HashSet<Envelope>>()
    			{

					@Override
					public Tuple2<Envelope, HashSet<Envelope>> call(
							Tuple2<Envelope, Iterable<Envelope>> t)
							throws Exception {
						Iterator<Envelope> objectIterator=t._2().iterator();
						HashSet<Envelope> result=new HashSet<Envelope>();
						while(objectIterator.hasNext())
						{
							result.add(objectIterator.next());
						}
						return new Tuple2<Envelope,HashSet<Envelope>>(t._1(),result);
					}
    		
    			});
    	return mapMissedResult;
	}

	public JavaPairRDD<Polygon,HashSet<Point>> handleMissingObjects(JavaSparkContext sc,PointRDD objectRDD,PolygonRDD queryWindowRDD)
	{
    	//This part is used to separate missed objects and covered objects by sample data based spatial partitioning.
    	final int gridSize=objectRDD.grids.size();
    	JavaPairRDD<Integer,Point> missedObjectRDDWithID=objectRDD.gridPointRDD.filter(new Function<Tuple2<Integer,Point>,Boolean>(){
			@Override
			public Boolean call(Tuple2<Integer, Point> object) throws Exception {
				// TODO Auto-generated method stub
				if(object._1()>=gridSize)
				{
					return true;
				}
				return false;
			}});
    	missedObjectRDDWithID.partitionBy(new SpatialPartitioner(gridSize));
    	JavaRDD<Point> missedObjectRDD=missedObjectRDDWithID.map(new Function<Tuple2<Integer,Point>, Point>()
    			{

					@Override
					public Point call(Tuple2<Integer, Point> object)
							throws Exception {
						return object._2();
					}
    		
    			});
    	JavaPairRDD<Integer,Polygon> missedWindowRDDWithID=queryWindowRDD.gridPolygonRDD.filter(new Function<Tuple2<Integer,Polygon>,Boolean>(){
			@Override
			public Boolean call(Tuple2<Integer, Polygon> object) throws Exception {
				// TODO Auto-generated method stub
				if(object._1()>=gridSize)
				{
					return true;
				}
				return false;
			}});
    	
    	JavaRDD<Polygon> missedWindowRDD=missedWindowRDDWithID.map(new Function<Tuple2<Integer,Polygon>, Polygon>()
    			{

					@Override
					public Polygon call(Tuple2<Integer, Polygon> object)
							throws Exception {
						return object._2();
					}
    		
    			});
    	List<Polygon> collectWindow=missedWindowRDD.collect();
    	final Broadcast<List<Polygon>> missedWindows = sc.broadcast(collectWindow);
    	JavaPairRDD<Polygon,Point> missedResult=missedObjectRDD.flatMapToPair(new PairFlatMapFunction<Point,Polygon,Point>()
    			{

					@Override
					public Iterable<Tuple2<Polygon, Point>> call(Point t)
							throws Exception {
						Iterator<Polygon> windowIterator=missedWindows.getValue().iterator();
						List<Tuple2<Polygon, Point>> result=new ArrayList<Tuple2<Polygon, Point>>();
						while(windowIterator.hasNext())
						{
							Polygon window=windowIterator.next();
							if(window.contains(t))
							{
								result.add(new Tuple2<Polygon,Point>(window,t));
							}
						}
						return result;
					}
    			});
    	JavaPairRDD<Polygon,Iterable<Point>> groupbyMissedResult=missedResult.groupByKey();
    	JavaPairRDD<Polygon,HashSet<Point>> mapMissedResult=groupbyMissedResult.mapToPair(new PairFunction<Tuple2<Polygon,Iterable<Point>>,Polygon,HashSet<Point>>()
    			{

					@Override
					public Tuple2<Polygon, HashSet<Point>> call(
							Tuple2<Polygon, Iterable<Point>> t)
							throws Exception {
						Iterator<Point> objectIterator=t._2().iterator();
						HashSet<Point> result=new HashSet<Point>();
						while(objectIterator.hasNext())
						{
							result.add(objectIterator.next());
						}
						return new Tuple2<Polygon,HashSet<Point>>(t._1(),result);
					}
    		
    			});
    	return mapMissedResult;
	}
	public JavaPairRDD<Polygon,HashSet<Point>> handleMissingObjectsUsingIndex(JavaSparkContext sc,PointRDD objectRDD,PolygonRDD queryWindowRDD)
	{
    	//This part is used to separate missed objects and covered objects by sample data based spatial partitioning.
    	final int gridSize=objectRDD.grids.size();
    	JavaPairRDD<Integer,STRtree> missedObjectRDDWithID=objectRDD.indexedRDD.filter(new Function<Tuple2<Integer,STRtree>,Boolean>(){
			@Override
			public Boolean call(Tuple2<Integer, STRtree> object) throws Exception {
				// TODO Auto-generated method stub
				if(object._1()>=gridSize)
				{
					return true;
				}
				return false;
			}});
    	missedObjectRDDWithID.partitionBy(new SpatialPartitioner(gridSize));
    	JavaRDD<STRtree> missedObjectRDD=missedObjectRDDWithID.map(new Function<Tuple2<Integer,STRtree>, STRtree>()
    			{

					@Override
					public STRtree call(Tuple2<Integer, STRtree> object)
							throws Exception {
						return object._2();
					}
    		
    			});
    	JavaPairRDD<Integer,Polygon> missedWindowRDDWithID=queryWindowRDD.gridPolygonRDD.filter(new Function<Tuple2<Integer,Polygon>,Boolean>(){
			@Override
			public Boolean call(Tuple2<Integer, Polygon> object) throws Exception {
				// TODO Auto-generated method stub
				if(object._1()>=gridSize)
				{
					return true;
				}
				return false;
			}});
    	
    	JavaRDD<Polygon> missedWindowRDD=missedWindowRDDWithID.map(new Function<Tuple2<Integer,Polygon>, Polygon>()
    			{

					@Override
					public Polygon call(Tuple2<Integer, Polygon> object)
							throws Exception {
						return object._2();
					}
    		
    			});
    	List<Polygon> collectWindow=missedWindowRDD.collect();
    	final Broadcast<List<Polygon>> missedWindows = sc.broadcast(collectWindow);
    	JavaPairRDD<Polygon,Point> missedResult=missedObjectRDD.flatMapToPair(new PairFlatMapFunction<STRtree,Polygon,Point>()
    			{

					@Override
					public Iterable<Tuple2<Polygon, Point>> call(STRtree t)
							throws Exception {
						Iterator<Polygon> windowIterator=missedWindows.getValue().iterator();
						List<Tuple2<Polygon, Point>> result=new ArrayList<Tuple2<Polygon, Point>>();
						while(windowIterator.hasNext())
						{
							Polygon window=windowIterator.next();
							List<Point> containedResult=t.query(window.getEnvelopeInternal());
							Iterator<Point> objectIterator= containedResult.iterator();
							while(objectIterator.hasNext())
							{
								result.add(new Tuple2<Polygon,Point>(window,objectIterator.next()));
							}	
						}
						return result;
					}
    			});
    	JavaPairRDD<Polygon,Iterable<Point>> groupbyMissedResult=missedResult.groupByKey();
    	JavaPairRDD<Polygon,HashSet<Point>> mapMissedResult=groupbyMissedResult.mapToPair(new PairFunction<Tuple2<Polygon,Iterable<Point>>,Polygon,HashSet<Point>>()
    			{

					@Override
					public Tuple2<Polygon, HashSet<Point>> call(
							Tuple2<Polygon, Iterable<Point>> t)
							throws Exception {
						Iterator<Point> objectIterator=t._2().iterator();
						HashSet<Point> result=new HashSet<Point>();
						while(objectIterator.hasNext())
						{
							result.add(objectIterator.next());
						}
						return new Tuple2<Polygon,HashSet<Point>>(t._1(),result);
					}
    		
    			});
    	return mapMissedResult;
	}

	public JavaPairRDD<Polygon,HashSet<Polygon>> handleMissingObjects(JavaSparkContext sc,PolygonRDD objectRDD,PolygonRDD queryWindowRDD)
	{
    	//This part is used to separate missed objects and covered objects by sample data based spatial partitioning.
    	final int gridSize=objectRDD.grids.size();
    	JavaPairRDD<Integer,Polygon> missedObjectRDDWithID=objectRDD.gridPolygonRDD.filter(new Function<Tuple2<Integer,Polygon>,Boolean>(){
			@Override
			public Boolean call(Tuple2<Integer, Polygon> object) throws Exception {
				// TODO Auto-generated method stub
				if(object._1()>=gridSize)
				{
					return true;
				}
				return false;
			}});
    	missedObjectRDDWithID.partitionBy(new SpatialPartitioner(gridSize));
    	JavaRDD<Polygon> missedObjectRDD=missedObjectRDDWithID.map(new Function<Tuple2<Integer,Polygon>, Polygon>()
    			{

					@Override
					public Polygon call(Tuple2<Integer, Polygon> object)
							throws Exception {
						return object._2();
					}
    		
    			});
    	JavaPairRDD<Integer,Polygon> missedWindowRDDWithID=queryWindowRDD.gridPolygonRDD.filter(new Function<Tuple2<Integer,Polygon>,Boolean>(){
			@Override
			public Boolean call(Tuple2<Integer, Polygon> object) throws Exception {
				// TODO Auto-generated method stub
				if(object._1()>=gridSize)
				{
					return true;
				}
				return false;
			}});
    	JavaRDD<Polygon> missedWindowRDD=missedWindowRDDWithID.map(new Function<Tuple2<Integer,Polygon>, Polygon>()
    			{

					@Override
					public Polygon call(Tuple2<Integer, Polygon> object)
							throws Exception {
						return object._2();
					}
    		
    			});
    	List<Polygon> collectWindow=missedWindowRDD.collect();
    	final Broadcast<List<Polygon>> missedWindows = sc.broadcast(collectWindow);
    	JavaPairRDD<Polygon,Polygon> missedResult=missedObjectRDD.flatMapToPair(new PairFlatMapFunction<Polygon,Polygon,Polygon>()
    			{

					@Override
					public Iterable<Tuple2<Polygon, Polygon>> call(Polygon t)
							throws Exception {
						Iterator<Polygon> windowIterator=missedWindows.getValue().iterator();
						List<Tuple2<Polygon, Polygon>> result=new ArrayList<Tuple2<Polygon, Polygon>>();
						while(windowIterator.hasNext())
						{
							Polygon window=windowIterator.next();
							if(window.contains(t))
							{
								result.add(new Tuple2<Polygon,Polygon>(window,t));
							}
						}
						return result;
					}
    			});
    	JavaPairRDD<Polygon,Iterable<Polygon>> groupbyMissedResult=missedResult.groupByKey();
    	JavaPairRDD<Polygon,HashSet<Polygon>> mapMissedResult=groupbyMissedResult.mapToPair(new PairFunction<Tuple2<Polygon,Iterable<Polygon>>,Polygon,HashSet<Polygon>>()
    			{

					@Override
					public Tuple2<Polygon, HashSet<Polygon>> call(
							Tuple2<Polygon, Iterable<Polygon>> t)
							throws Exception {
						Iterator<Polygon> objectIterator=t._2().iterator();
						HashSet<Polygon> result=new HashSet<Polygon>();
						while(objectIterator.hasNext())
						{
							result.add(objectIterator.next());
						}
						return new Tuple2<Polygon,HashSet<Polygon>>(t._1(),result);
					}
    		
    			});
    	return mapMissedResult;
	}
	public JavaPairRDD<Polygon,HashSet<Polygon>> handleMissingObjectsUsingIndex(JavaSparkContext sc, PolygonRDD objectRDD,PolygonRDD queryWindowRDD)
	{
    	//This part is used to separate missed objects and covered objects by sample data based spatial partitioning.
    	final int gridSize=objectRDD.grids.size();
    	JavaPairRDD<Integer,STRtree> missedObjectRDDWithID=objectRDD.indexedRDD.filter(new Function<Tuple2<Integer,STRtree>,Boolean>(){
			@Override
			public Boolean call(Tuple2<Integer, STRtree> object) throws Exception {
				// TODO Auto-generated method stub
				if(object._1()>=gridSize)
				{
					return true;
				}
				return false;
			}});
    	missedObjectRDDWithID.partitionBy(new SpatialPartitioner(gridSize));
    	JavaRDD<STRtree> missedObjectRDD=missedObjectRDDWithID.map(new Function<Tuple2<Integer,STRtree>, STRtree>()
    			{

					@Override
					public STRtree call(Tuple2<Integer, STRtree> object)
							throws Exception {
						return object._2();
					}
    		
    			});
    	JavaPairRDD<Integer,Polygon> missedWindowRDDWithID=queryWindowRDD.gridPolygonRDD.filter(new Function<Tuple2<Integer,Polygon>,Boolean>(){
			@Override
			public Boolean call(Tuple2<Integer, Polygon> object) throws Exception {
				// TODO Auto-generated method stub
				if(object._1()>=gridSize)
				{
					return true;
				}
				return false;
			}});
    	JavaRDD<Polygon> missedWindowRDD=missedWindowRDDWithID.map(new Function<Tuple2<Integer,Polygon>, Polygon>()
    			{

					@Override
					public Polygon call(Tuple2<Integer, Polygon> object)
							throws Exception {
						return object._2();
					}
    		
    			});
    	List<Polygon> collectWindow=missedWindowRDD.collect();
    	final Broadcast<List<Polygon>> missedWindows = sc.broadcast(collectWindow);
    	JavaPairRDD<Polygon,Polygon> missedResult=missedObjectRDD.flatMapToPair(new PairFlatMapFunction<STRtree,Polygon,Polygon>()
    			{

					@Override
					public Iterable<Tuple2<Polygon, Polygon>> call(STRtree t)
							throws Exception {
						Iterator<Polygon> windowIterator=missedWindows.getValue().iterator();
						List<Tuple2<Polygon, Polygon>> result=new ArrayList<Tuple2<Polygon, Polygon>>();
						while(windowIterator.hasNext())
						{
							Polygon window=windowIterator.next();
							List<Polygon> containedResult=t.query(window.getEnvelopeInternal());
							Iterator<Polygon> objectIterator= containedResult.iterator();
							while(objectIterator.hasNext())
							{
								result.add(new Tuple2<Polygon,Polygon>(window,objectIterator.next()));
							}	
						}
						return result;
					}
    			});
    	JavaPairRDD<Polygon,Iterable<Polygon>> groupbyMissedResult=missedResult.groupByKey();
    	JavaPairRDD<Polygon,HashSet<Polygon>> mapMissedResult=groupbyMissedResult.mapToPair(new PairFunction<Tuple2<Polygon,Iterable<Polygon>>,Polygon,HashSet<Polygon>>()
    			{

					@Override
					public Tuple2<Polygon, HashSet<Polygon>> call(
							Tuple2<Polygon, Iterable<Polygon>> t)
							throws Exception {
						Iterator<Polygon> objectIterator=t._2().iterator();
						HashSet<Polygon> result=new HashSet<Polygon>();
						while(objectIterator.hasNext())
						{
							result.add(objectIterator.next());
						}
						return new Tuple2<Polygon,HashSet<Polygon>>(t._1(),result);
					}
    		
    			});
    	return mapMissedResult;
	}

	
}
