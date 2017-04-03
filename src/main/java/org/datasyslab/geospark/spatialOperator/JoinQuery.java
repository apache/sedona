/**
 * FILE: JoinQuery.java
 * PATH: org.datasyslab.geospark.spatialOperator.JoinQuery.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialOperator;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.datasyslab.geospark.geometryObjects.Circle;
import org.datasyslab.geospark.joinJudgement.GeometryByCircleJudgement;
import org.datasyslab.geospark.joinJudgement.GeometryByCircleJudgementUsingIndex;
import org.datasyslab.geospark.joinJudgement.GeometryByPolygonJudgement;
import org.datasyslab.geospark.joinJudgement.GeometryByPolygonJudgementUsingIndex;

import org.datasyslab.geospark.spatialPartitioning.DuplicatesHandler;
import org.datasyslab.geospark.spatialRDD.CircleRDD;
import org.datasyslab.geospark.spatialRDD.LineStringRDD;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.SpatialIndex;

import scala.Tuple2;

/**
 * The Class JoinQuery.
 */
public class JoinQuery implements Serializable{

	/**
	 * Execute spatial join using index.
	 *
	 * @param spatialRDD the spatial RDD
	 * @param queryRDD the query RDD
	 * @param considerBoundaryIntersection the consider boundary intersection
	 * @return the java pair RDD
	 * @throws Exception the exception
	 */
	private static JavaPairRDD<Polygon, HashSet<Geometry>> executeSpatialJoinUsingIndex(SpatialRDD spatialRDD,SpatialRDD queryRDD,boolean considerBoundaryIntersection) throws Exception
	{
    	//Check if rawPointRDD have index.
        if(spatialRDD.indexedRDD == null) {
            throw new Exception("[JoinQuery][SpatialJoinQuery] Index doesn't exist. Please build index.");
        }
        if(spatialRDD.spatialPartitionedRDD == null) {
            throw new Exception("[JoinQuery][SpatialJoinQuery]spatialRDD SpatialPartitionedRDD is null. Please do spatial partitioning.");
        }
        else if(queryRDD.spatialPartitionedRDD == null)
        {
            throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD SpatialPartitionedRDD is null. Please use the spatialRDD's grids to do spatial partitioning.");
        }
        else if(queryRDD.grids.equals(spatialRDD.grids)==false)
        {
            throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD is not partitioned by the same grids with spatialRDD. Please make sure they both use the same grids otherwise wrong results will appear.");
        }
        JavaPairRDD<Integer, Tuple2<Iterable<SpatialIndex>, Iterable<Object>>> cogroupResult = spatialRDD.indexedRDD.cogroup(queryRDD.spatialPartitionedRDD);

        //flatMapToPair, use HashSet.

        JavaPairRDD<Polygon, HashSet<Geometry>> joinResultWithDuplicates = cogroupResult.flatMapToPair(new GeometryByPolygonJudgementUsingIndex(considerBoundaryIntersection));
        
        return DuplicatesHandler.removeDuplicatesGeometryByPolygon(joinResultWithDuplicates);
	}
	
	/**
	 * Execute spatial join no index.
	 *
	 * @param spatialRDD the spatial RDD
	 * @param queryRDD the query RDD
	 * @param considerBoundaryIntersection the consider boundary intersection
	 * @return the java pair RDD
	 * @throws Exception the exception
	 */
	private static JavaPairRDD<Polygon, HashSet<Geometry>> executeSpatialJoinNoIndex(SpatialRDD spatialRDD,SpatialRDD queryRDD,boolean considerBoundaryIntersection) throws Exception
	{
        if(spatialRDD.spatialPartitionedRDD == null) {
            throw new Exception("[JoinQuery][SpatialJoinQuery]spatialRDD SpatialPartitionedRDD is null. Please do spatial partitioning.");
        }
        else if(queryRDD.spatialPartitionedRDD == null)
        {
            throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD SpatialPartitionedRDD is null. Please use the spatialRDD's grids to do spatial partitioning.");
        }
        else if(queryRDD.grids.equals(spatialRDD.grids)==false)
        {
            throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD is not partitioned by the same grids with spatialRDD. Please make sure they both use the same grids otherwise wrong results will appear.");
        }
        JavaPairRDD<Integer, Tuple2<Iterable<Object>, Iterable<Object>>> cogroupResult = spatialRDD.spatialPartitionedRDD.cogroup(queryRDD.spatialPartitionedRDD);
            
        //flatMapToPair, use HashSet.

        JavaPairRDD<Polygon, HashSet<Geometry>> joinResultWithDuplicates = cogroupResult.flatMapToPair(new GeometryByPolygonJudgement(considerBoundaryIntersection));
        
        return DuplicatesHandler.removeDuplicatesGeometryByPolygon(joinResultWithDuplicates);
	}
	
	/**
	 * Execute distance join using index.
	 *
	 * @param spatialRDD the spatial RDD
	 * @param queryRDD the query RDD
	 * @param considerBoundaryIntersection the consider boundary intersection
	 * @return the java pair RDD
	 * @throws Exception the exception
	 */
	private static JavaPairRDD<Circle, HashSet<Geometry>> executeDistanceJoinUsingIndex(SpatialRDD spatialRDD,SpatialRDD queryRDD,boolean considerBoundaryIntersection) throws Exception
	{
    	//Check if rawPointRDD have index.
        if(spatialRDD.indexedRDD == null) {
            throw new Exception("[JoinQuery][SpatialJoinQuery] Index doesn't exist. Please build index.");
        }
        if(spatialRDD.spatialPartitionedRDD == null) {
            throw new Exception("[JoinQuery][SpatialJoinQuery]spatialRDD SpatialPartitionedRDD is null. Please do spatial partitioning.");
        }
        else if(queryRDD.spatialPartitionedRDD == null)
        {
            throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD SpatialPartitionedRDD is null. Please use the spatialRDD's grids to do spatial partitioning.");
        }
        else if(queryRDD.grids.equals(spatialRDD.grids)==false)
        {
            throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD is not partitioned by the same grids with spatialRDD. Please make sure they both use the same grids otherwise wrong results will appear.");
        }
        JavaPairRDD<Integer, Tuple2<Iterable<SpatialIndex>, Iterable<Object>>> cogroupResult = spatialRDD.indexedRDD.cogroup(queryRDD.spatialPartitionedRDD);

        //flatMapToPair, use HashSet.

        JavaPairRDD<Circle, HashSet<Geometry>> joinResultWithDuplicates = cogroupResult.flatMapToPair(new GeometryByCircleJudgementUsingIndex(considerBoundaryIntersection));
        
        return DuplicatesHandler.removeDuplicatesGeometryByCircle(joinResultWithDuplicates);
	}
	
	/**
	 * Execute distance join no index.
	 *
	 * @param spatialRDD the spatial RDD
	 * @param queryRDD the query RDD
	 * @param considerBoundaryIntersection the consider boundary intersection
	 * @return the java pair RDD
	 * @throws Exception the exception
	 */
	private static JavaPairRDD<Circle, HashSet<Geometry>> executeDistanceJoinNoIndex(SpatialRDD spatialRDD,SpatialRDD queryRDD,boolean considerBoundaryIntersection) throws Exception
	{
        if(spatialRDD.spatialPartitionedRDD == null) {
            throw new Exception("[JoinQuery][SpatialJoinQuery]spatialRDD SpatialPartitionedRDD is null. Please do spatial partitioning.");
        }
        else if(queryRDD.spatialPartitionedRDD == null)
        {
            throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD SpatialPartitionedRDD is null. Please use the spatialRDD's grids to do spatial partitioning.");
        }
        else if(queryRDD.grids.equals(spatialRDD.grids)==false)
        {
            throw new Exception("[JoinQuery][SpatialJoinQuery]queryRDD is not partitioned by the same grids with spatialRDD. Please make sure they both use the same grids otherwise wrong results will appear.");
        }
        JavaPairRDD<Integer, Tuple2<Iterable<Object>, Iterable<Object>>> cogroupResult = spatialRDD.spatialPartitionedRDD.cogroup(queryRDD.spatialPartitionedRDD);
            
        //flatMapToPair, use HashSet.

        JavaPairRDD<Circle, HashSet<Geometry>> joinResultWithDuplicates = cogroupResult.flatMapToPair(new GeometryByCircleJudgement(considerBoundaryIntersection));
        
        return DuplicatesHandler.removeDuplicatesGeometryByCircle(joinResultWithDuplicates);
	}
	
    /**
     * Spatial join query.
     *
     * @param spatialRDD the spatial RDD
     * @param queryRDD the query RDD
     * @param useIndex the use index
     * @param considerBoundaryIntersection the consider boundary intersection
     * @return the java pair RDD
     * @throws Exception the exception
     */
    public static JavaPairRDD<Polygon, HashSet<Point>> SpatialJoinQuery(PointRDD spatialRDD,RectangleRDD queryRDD,boolean useIndex,boolean considerBoundaryIntersection) throws Exception {

        if(useIndex)
        {
        	JavaPairRDD<Polygon, HashSet<Geometry>> joinListResultAfterAggregation = executeSpatialJoinUsingIndex(spatialRDD,queryRDD,considerBoundaryIntersection);
            JavaPairRDD<Polygon, HashSet<Point>> castedResult = joinListResultAfterAggregation.mapValues(new Function<HashSet<Geometry>,HashSet<Point>>()
            {
				@Override
				public HashSet<Point> call(HashSet<Geometry> spatialObjects) throws Exception {
					HashSet<Point> castedSpatialObjects = new HashSet<Point>();
					Iterator spatialObjectIterator = spatialObjects.iterator();
					while(spatialObjectIterator.hasNext())
					{
						castedSpatialObjects.add((Point)spatialObjectIterator.next());
					}
					return castedSpatialObjects;
				}
            	
            });
            return castedResult;
        }
        else
        {

        	JavaPairRDD<Polygon, HashSet<Geometry>> joinListResultAfterAggregation = executeSpatialJoinNoIndex(spatialRDD,queryRDD,considerBoundaryIntersection);
            JavaPairRDD<Polygon, HashSet<Point>> castedResult = joinListResultAfterAggregation.mapValues(new Function<HashSet<Geometry>,HashSet<Point>>()
            {
				@Override
				public HashSet<Point> call(HashSet<Geometry> spatialObjects) throws Exception {
					HashSet<Point> castedSpatialObjects = new HashSet<Point>();
					Iterator spatialObjectIterator = spatialObjects.iterator();
					while(spatialObjectIterator.hasNext())
					{
						castedSpatialObjects.add((Point)spatialObjectIterator.next());
					}
					return castedSpatialObjects;
				}
            	
            });
            return castedResult;
        }
    }
    
 

    /**
     * Spatial join query.
     *
     * @param spatialRDD the spatial RDD
     * @param queryRDD the query RDD
     * @param useIndex the use index
     * @param considerBoundaryIntersection the consider boundary intersection
     * @return the java pair RDD
     * @throws Exception the exception
     */
    public static JavaPairRDD<Polygon, HashSet<Polygon>> SpatialJoinQuery(RectangleRDD spatialRDD,RectangleRDD queryRDD,boolean useIndex,boolean considerBoundaryIntersection) throws Exception {

        if(useIndex)
        {
        	JavaPairRDD<Polygon, HashSet<Geometry>> joinListResultAfterAggregation = executeSpatialJoinUsingIndex(spatialRDD,queryRDD,considerBoundaryIntersection);
            JavaPairRDD<Polygon, HashSet<Polygon>> castedResult = joinListResultAfterAggregation.mapValues(new Function<HashSet<Geometry>,HashSet<Polygon>>()
            {
				@Override
				public HashSet<Polygon> call(HashSet<Geometry> spatialObjects) throws Exception {
					HashSet<Polygon> castedSpatialObjects = new HashSet<Polygon>();
					Iterator spatialObjectIterator = spatialObjects.iterator();
					while(spatialObjectIterator.hasNext())
					{
						castedSpatialObjects.add((Polygon)spatialObjectIterator.next());
					}
					return castedSpatialObjects;
				}
            	
            });
                        
            return castedResult;
        }
        else
        {
        	JavaPairRDD<Polygon, HashSet<Geometry>> joinListResultAfterAggregation = executeSpatialJoinNoIndex(spatialRDD,queryRDD,considerBoundaryIntersection);

            JavaPairRDD<Polygon, HashSet<Polygon>> castedResult = joinListResultAfterAggregation.mapValues(new Function<HashSet<Geometry>,HashSet<Polygon>>()
            {
				@Override
				public HashSet<Polygon> call(HashSet<Geometry> spatialObjects) throws Exception {
					HashSet<Polygon> castedSpatialObjects = new HashSet<Polygon>();
					Iterator spatialObjectIterator = spatialObjects.iterator();
					while(spatialObjectIterator.hasNext())
					{
						castedSpatialObjects.add((Polygon)spatialObjectIterator.next());
					}
					return castedSpatialObjects;
				}
            	
            });
            return castedResult;
        }
    }
   
    /**
     * Spatial join query.
     *
     * @param spatialRDD the spatial RDD
     * @param queryRDD the query RDD
     * @param useIndex the use index
     * @param considerBoundaryIntersection the consider boundary intersection
     * @return the java pair RDD
     * @throws Exception the exception
     */
    public static JavaPairRDD<Polygon, HashSet<Point>> SpatialJoinQuery(PointRDD spatialRDD,PolygonRDD queryRDD, boolean useIndex,boolean considerBoundaryIntersection) throws Exception {
        if(useIndex)
        {
        	JavaPairRDD<Polygon, HashSet<Geometry>> joinListResultAfterAggregation = executeSpatialJoinUsingIndex(spatialRDD,queryRDD,considerBoundaryIntersection); 
            JavaPairRDD<Polygon, HashSet<Point>> castedResult = joinListResultAfterAggregation.mapToPair(new PairFunction<Tuple2<Polygon,HashSet<Geometry>>,Polygon,HashSet<Point>>()
            {
				@Override
				public Tuple2<Polygon, HashSet<Point>> call(Tuple2<Polygon, HashSet<Geometry>> pairObjects) throws Exception {
					HashSet<Point> castedSpatialObjects = new HashSet<Point>();
					Iterator<Geometry> spatialObjectIterator = pairObjects._2.iterator();
					while(spatialObjectIterator.hasNext())
					{
						Point castedSpatialObject = (Point)spatialObjectIterator.next();
						castedSpatialObjects.add(castedSpatialObject);
					}
					return new Tuple2<Polygon,HashSet<Point>>(pairObjects._1,castedSpatialObjects);
				}
            });
            return castedResult;
        }
        else
        {
        	JavaPairRDD<Polygon, HashSet<Geometry>> joinListResultAfterAggregation = executeSpatialJoinNoIndex(spatialRDD,queryRDD,considerBoundaryIntersection);             
            JavaPairRDD<Polygon, HashSet<Point>> castedResult = joinListResultAfterAggregation.mapValues(new Function<HashSet<Geometry>,HashSet<Point>>()
            {
				@Override
				public HashSet<Point> call(HashSet<Geometry> spatialObjects) throws Exception {
					HashSet<Point> castedSpatialObjects = new HashSet<Point>();
					Iterator spatialObjectIterator = spatialObjects.iterator();
					while(spatialObjectIterator.hasNext())
					{
						castedSpatialObjects.add((Point)spatialObjectIterator.next());
					}
					return castedSpatialObjects;
				}
            });
            return castedResult;
        }

   }
   

   
    /**
     * Spatial join query.
     *
     * @param spatialRDD the spatial RDD
     * @param queryRDD the query RDD
     * @param useIndex the use index
     * @param considerBoundaryIntersection the consider boundary intersection
     * @return the java pair RDD
     * @throws Exception the exception
     */
    public static JavaPairRDD<Polygon, HashSet<Polygon>> SpatialJoinQuery(PolygonRDD spatialRDD,PolygonRDD queryRDD, boolean useIndex,boolean considerBoundaryIntersection) throws Exception {
        if(useIndex)
        {
        	JavaPairRDD<Polygon, HashSet<Geometry>> joinListResultAfterAggregation = executeSpatialJoinUsingIndex(spatialRDD,queryRDD,considerBoundaryIntersection);             
            JavaPairRDD<Polygon, HashSet<Polygon>> castedResult = joinListResultAfterAggregation.mapToPair(new PairFunction<Tuple2<Polygon,HashSet<Geometry>>,Polygon,HashSet<Polygon>>()
            {
				@Override
				public Tuple2<Polygon, HashSet<Polygon>> call(Tuple2<Polygon, HashSet<Geometry>> pairObjects) throws Exception {
					HashSet<Polygon> castedSpatialObjects = new HashSet<Polygon>();
					Iterator<Geometry> spatialObjectIterator = pairObjects._2.iterator();
					while(spatialObjectIterator.hasNext())
					{
						Polygon castedSpatialObject = (Polygon)spatialObjectIterator.next();
						castedSpatialObjects.add(castedSpatialObject);
					}
					return new Tuple2<Polygon,HashSet<Polygon>>(pairObjects._1,castedSpatialObjects);
				}
            });
            return castedResult;
        }
        else
        {
        	JavaPairRDD<Polygon, HashSet<Geometry>> joinListResultAfterAggregation = executeSpatialJoinNoIndex(spatialRDD,queryRDD,considerBoundaryIntersection); 

            JavaPairRDD<Polygon, HashSet<Polygon>> castedResult = joinListResultAfterAggregation.mapValues(new Function<HashSet<Geometry>,HashSet<Polygon>>()
            {
				@Override
				public HashSet<Polygon> call(HashSet<Geometry> spatialObjects) throws Exception {
					HashSet<Polygon> castedSpatialObjects = new HashSet<Polygon>();
					Iterator spatialObjectIterator = spatialObjects.iterator();
					while(spatialObjectIterator.hasNext())
					{
						castedSpatialObjects.add((Polygon)spatialObjectIterator.next());
					}
					return castedSpatialObjects;
				}
            });
            return castedResult;
        }

   }

    /**
     * Spatial join query.
     *
     * @param spatialRDD the spatial RDD
     * @param queryRDD the query RDD
     * @param useIndex the use index
     * @param considerBoundaryIntersection the consider boundary intersection
     * @return the java pair RDD
     * @throws Exception the exception
     */
    public static JavaPairRDD<Polygon, HashSet<LineString>> SpatialJoinQuery(LineStringRDD spatialRDD,PolygonRDD queryRDD, boolean useIndex,boolean considerBoundaryIntersection) throws Exception {
        if(useIndex)
        {
        	JavaPairRDD<Polygon, HashSet<Geometry>> joinListResultAfterAggregation = executeSpatialJoinUsingIndex(spatialRDD,queryRDD,considerBoundaryIntersection); 
            JavaPairRDD<Polygon, HashSet<LineString>> castedResult = joinListResultAfterAggregation.mapToPair(new PairFunction<Tuple2<Polygon,HashSet<Geometry>>,Polygon,HashSet<LineString>>()
            {
				@Override
				public Tuple2<Polygon, HashSet<LineString>> call(Tuple2<Polygon, HashSet<Geometry>> pairObjects) throws Exception {
					HashSet<LineString> castedSpatialObjects = new HashSet<LineString>();
					Iterator<Geometry> spatialObjectIterator = pairObjects._2.iterator();
					while(spatialObjectIterator.hasNext())
					{
						LineString castedSpatialObject = (LineString)spatialObjectIterator.next();
						castedSpatialObjects.add(castedSpatialObject);
					}
					return new Tuple2<Polygon,HashSet<LineString>>(pairObjects._1,castedSpatialObjects);
				}
            });
            return castedResult;
        }
        else
        {
        	JavaPairRDD<Polygon, HashSet<Geometry>> joinListResultAfterAggregation = executeSpatialJoinNoIndex(spatialRDD,queryRDD,considerBoundaryIntersection);             
            JavaPairRDD<Polygon, HashSet<LineString>> castedResult = joinListResultAfterAggregation.mapValues(new Function<HashSet<Geometry>,HashSet<LineString>>()
            {
				@Override
				public HashSet<LineString> call(HashSet<Geometry> spatialObjects) throws Exception {
					HashSet<LineString> castedSpatialObjects = new HashSet<LineString>();
					Iterator spatialObjectIterator = spatialObjects.iterator();
					while(spatialObjectIterator.hasNext())
					{
						castedSpatialObjects.add((LineString)spatialObjectIterator.next());
					}
					return castedSpatialObjects;
				}
            });
            return castedResult;
        }

   }

    /**
     * Spatial join query count by key.
     *
     * @param spatialRDD the spatial RDD
     * @param queryRDD the query RDD
     * @param useIndex the use index
     * @param considerBoundaryIntersection the consider boundary intersection
     * @return the java pair RDD
     * @throws Exception the exception
     */
    public static JavaPairRDD<Polygon, Long> SpatialJoinQueryCountByKey(SpatialRDD spatialRDD,PolygonRDD queryRDD, boolean useIndex,boolean considerBoundaryIntersection) throws Exception {
        if(useIndex)
        {
        	JavaPairRDD<Polygon, HashSet<Geometry>> joinListResultAfterAggregation = executeSpatialJoinUsingIndex(spatialRDD,queryRDD,considerBoundaryIntersection);             
            JavaPairRDD<Polygon, Long> resultCountByKey = joinListResultAfterAggregation.mapToPair(new PairFunction<Tuple2<Polygon,HashSet<Geometry>>,Polygon,Long>()
            {
				@Override
				public Tuple2<Polygon, Long> call(Tuple2<Polygon, HashSet<Geometry>> pairObjects) throws Exception {
					Iterator<Geometry> spatialObjectIterator = pairObjects._2.iterator();
					long count=0;
					while(spatialObjectIterator.hasNext())
					{
						Geometry castedSpatialObject = (Geometry)spatialObjectIterator.next();
						count++;
					}
					return new Tuple2<Polygon,Long>(pairObjects._1,count);
				}
            });
            return resultCountByKey;
        }
        else
        {
        	JavaPairRDD<Polygon, HashSet<Geometry>> joinListResultAfterAggregation = executeSpatialJoinNoIndex(spatialRDD,queryRDD,considerBoundaryIntersection); 
            JavaPairRDD<Polygon, Long> resultCountByKey = joinListResultAfterAggregation.mapValues(new Function<HashSet<Geometry>,Long>()
            {
				@Override
				public Long call(HashSet<Geometry> spatialObjects) throws Exception {

					return (long) spatialObjects.size();
				}
            });
            return resultCountByKey;
        }

   }
    
    
    /**
     * Spatial join query count by key.
     *
     * @param spatialRDD the spatial RDD
     * @param queryRDD the query RDD
     * @param useIndex the use index
     * @param considerBoundaryIntersection the consider boundary intersection
     * @return the java pair RDD
     * @throws Exception the exception
     */
    public static JavaPairRDD<Polygon, Long> SpatialJoinQueryCountByKey(SpatialRDD spatialRDD,RectangleRDD queryRDD,boolean useIndex,boolean considerBoundaryIntersection) throws Exception {

        if(useIndex)
        {
        	JavaPairRDD<Polygon, HashSet<Geometry>> joinListResultAfterAggregation = executeSpatialJoinUsingIndex(spatialRDD,queryRDD,considerBoundaryIntersection);     
            JavaPairRDD<Polygon, Long> resultCountByKey = joinListResultAfterAggregation.mapValues(new Function<HashSet<Geometry>,Long>()
            {
				@Override
				public Long call(HashSet<Geometry> spatialObjects) throws Exception {
					return (long) spatialObjects.size();
				}
            	
            });
            return resultCountByKey;
        }
        else
        {
        	JavaPairRDD<Polygon, HashSet<Geometry>> joinListResultAfterAggregation = executeSpatialJoinNoIndex(spatialRDD,queryRDD,considerBoundaryIntersection);             
            JavaPairRDD<Polygon, Long> resultCountByKey = joinListResultAfterAggregation.mapValues(new Function<HashSet<Geometry>,Long>()
            {
				@Override
				public Long call(HashSet<Geometry> spatialObjects) throws Exception {
					return (long) spatialObjects.size();
				}
            });
            return resultCountByKey;
        }
    }

    /**
     * Distance join query.
     *
     * @param spatialRDD the spatial RDD
     * @param queryRDD the query RDD
     * @param useIndex the use index
     * @param considerBoundaryIntersection the consider boundary intersection
     * @return the java pair RDD
     * @throws Exception the exception
     */
    public static JavaPairRDD<Polygon, HashSet<Polygon>> DistanceJoinQuery(PolygonRDD spatialRDD,CircleRDD queryRDD, boolean useIndex,boolean considerBoundaryIntersection) throws Exception {
        if(useIndex)
        {
        	JavaPairRDD<Circle, HashSet<Geometry>> joinListResultAfterAggregation = executeDistanceJoinUsingIndex(spatialRDD,queryRDD,considerBoundaryIntersection); 
            JavaPairRDD<Polygon, HashSet<Polygon>> castedResult = joinListResultAfterAggregation.mapToPair(new PairFunction<Tuple2<Circle,HashSet<Geometry>>,Polygon,HashSet<Polygon>>()
            {
				@Override
				public Tuple2<Polygon, HashSet<Polygon>> call(Tuple2<Circle, HashSet<Geometry>> pairObjects) throws Exception {
					HashSet<Polygon> castedSpatialObjects = new HashSet<Polygon>();
					Iterator<Geometry> spatialObjectIterator = pairObjects._2.iterator();
					while(spatialObjectIterator.hasNext())
					{
						Polygon castedSpatialObject = (Polygon)spatialObjectIterator.next();
						castedSpatialObjects.add(castedSpatialObject);
					}
					return new Tuple2<Polygon,HashSet<Polygon>>((Polygon)pairObjects._1.getCenterGeometry(),castedSpatialObjects);
				}
            });
            return castedResult;
        }
        else
        {
        	JavaPairRDD<Circle, HashSet<Geometry>> joinListResultAfterAggregation = executeDistanceJoinNoIndex(spatialRDD,queryRDD,considerBoundaryIntersection); 
        	JavaPairRDD<Polygon, HashSet<Polygon>> castedResult = joinListResultAfterAggregation.mapToPair(new PairFunction<Tuple2<Circle,HashSet<Geometry>>,Polygon,HashSet<Polygon>>()
            {
				@Override
				public Tuple2<Polygon, HashSet<Polygon>> call(Tuple2<Circle, HashSet<Geometry>> pairObjects) throws Exception {
					HashSet<Polygon> castedSpatialObjects = new HashSet<Polygon>();
					Iterator<Geometry> spatialObjectIterator = pairObjects._2.iterator();
					while(spatialObjectIterator.hasNext())
					{
						Polygon castedSpatialObject = (Polygon)spatialObjectIterator.next();
						castedSpatialObjects.add(castedSpatialObject);
					}
					return new Tuple2<Polygon,HashSet<Polygon>>((Polygon)pairObjects._1.getCenterGeometry(),castedSpatialObjects);
				}
            });
            return castedResult;
        }
   }
    

    
 
    
    /**
     * Distance join query.
     *
     * @param spatialRDD the spatial RDD
     * @param queryRDD the query RDD
     * @param useIndex the use index
     * @param considerBoundaryIntersection the consider boundary intersection
     * @return the java pair RDD
     * @throws Exception the exception
     */
    public static JavaPairRDD<Point, HashSet<Point>> DistanceJoinQuery(PointRDD spatialRDD,PointRDD queryRDD, boolean useIndex,boolean considerBoundaryIntersection) throws Exception {
        if(useIndex)
        {
        	JavaPairRDD<Circle, HashSet<Geometry>> joinListResultAfterAggregation = executeDistanceJoinUsingIndex(spatialRDD,queryRDD,considerBoundaryIntersection); 
            JavaPairRDD<Point, HashSet<Point>> castedResult = joinListResultAfterAggregation.mapToPair(new PairFunction<Tuple2<Circle,HashSet<Geometry>>,Point,HashSet<Point>>()
            {
				@Override
				public Tuple2<Point, HashSet<Point>> call(Tuple2<Circle, HashSet<Geometry>> pairObjects) throws Exception {
					HashSet<Point> castedSpatialObjects = new HashSet<Point>();
					Iterator<Geometry> spatialObjectIterator = pairObjects._2.iterator();
					while(spatialObjectIterator.hasNext())
					{
						Point castedSpatialObject = (Point)spatialObjectIterator.next();
						castedSpatialObjects.add(castedSpatialObject);
					}
					return new Tuple2<Point,HashSet<Point>>((Point)pairObjects._1.getCenterGeometry(),castedSpatialObjects);
				}
            });
            return castedResult;
        }
        else
        {
        	JavaPairRDD<Circle, HashSet<Geometry>> joinListResultAfterAggregation = executeDistanceJoinNoIndex(spatialRDD,queryRDD,considerBoundaryIntersection); 
        	JavaPairRDD<Point, HashSet<Point>> castedResult = joinListResultAfterAggregation.mapToPair(new PairFunction<Tuple2<Circle,HashSet<Geometry>>,Point,HashSet<Point>>()
            {
				@Override
				public Tuple2<Point, HashSet<Point>> call(Tuple2<Circle, HashSet<Geometry>> pairObjects) throws Exception {
					HashSet<Point> castedSpatialObjects = new HashSet<Point>();
					Iterator<Geometry> spatialObjectIterator = pairObjects._2.iterator();
					while(spatialObjectIterator.hasNext())
					{
						Point castedSpatialObject = (Point)spatialObjectIterator.next();
						castedSpatialObjects.add(castedSpatialObject);
					}
					return new Tuple2<Point,HashSet<Point>>((Point)pairObjects._1.getCenterGeometry(),castedSpatialObjects);
				}
            });
            return castedResult;
        }
   }
    
    /**
     * Distance join query.
     *
     * @param spatialRDD the spatial RDD
     * @param queryRDD the query RDD
     * @param useIndex the use index
     * @param considerBoundaryIntersection the consider boundary intersection
     * @return the java pair RDD
     * @throws Exception the exception
     */
    public static JavaPairRDD<LineString, HashSet<LineString>> DistanceJoinQuery(LineStringRDD spatialRDD,LineStringRDD queryRDD, boolean useIndex,boolean considerBoundaryIntersection) throws Exception {
        if(useIndex)
        {
        	JavaPairRDD<Circle, HashSet<Geometry>> joinListResultAfterAggregation = executeDistanceJoinUsingIndex(spatialRDD,queryRDD,considerBoundaryIntersection); 
            JavaPairRDD<LineString, HashSet<LineString>> castedResult = joinListResultAfterAggregation.mapToPair(new PairFunction<Tuple2<Circle,HashSet<Geometry>>,LineString,HashSet<LineString>>()
            {
				@Override
				public Tuple2<LineString, HashSet<LineString>> call(Tuple2<Circle, HashSet<Geometry>> pairObjects) throws Exception {
					HashSet<LineString> castedSpatialObjects = new HashSet<LineString>();
					Iterator<Geometry> spatialObjectIterator = pairObjects._2.iterator();
					while(spatialObjectIterator.hasNext())
					{
						LineString castedSpatialObject = (LineString)spatialObjectIterator.next();
						castedSpatialObjects.add(castedSpatialObject);
					}
					return new Tuple2<LineString,HashSet<LineString>>((LineString)pairObjects._1.getCenterGeometry(),castedSpatialObjects);
				}
            });
            return castedResult;
        }
        else
        {
        	JavaPairRDD<Circle, HashSet<Geometry>> joinListResultAfterAggregation = executeDistanceJoinNoIndex(spatialRDD,queryRDD,considerBoundaryIntersection); 
        	JavaPairRDD<LineString, HashSet<LineString>> castedResult = joinListResultAfterAggregation.mapToPair(new PairFunction<Tuple2<Circle,HashSet<Geometry>>,LineString,HashSet<LineString>>()
            {
				@Override
				public Tuple2<LineString, HashSet<LineString>> call(Tuple2<Circle, HashSet<Geometry>> pairObjects) throws Exception {
					HashSet<LineString> castedSpatialObjects = new HashSet<LineString>();
					Iterator<Geometry> spatialObjectIterator = pairObjects._2.iterator();
					while(spatialObjectIterator.hasNext())
					{
						LineString castedSpatialObject = (LineString)spatialObjectIterator.next();
						castedSpatialObjects.add(castedSpatialObject);
					}
					return new Tuple2<LineString,HashSet<LineString>>((LineString)pairObjects._1.getCenterGeometry(),castedSpatialObjects);
				}
            });
            return castedResult;
        }
   }
 
    /**
     * Distance join query count by key.
     *
     * @param spatialRDD the spatial RDD
     * @param queryRDD the query RDD
     * @param useIndex the use index
     * @param considerBoundaryIntersection the consider boundary intersection
     * @return the java pair RDD
     * @throws Exception the exception
     */
    public static JavaPairRDD<Polygon, Long> DistanceJoinQueryCountByKey(PolygonRDD spatialRDD,CircleRDD queryRDD, boolean useIndex,boolean considerBoundaryIntersection) throws Exception {
        if(useIndex)
        {
        	JavaPairRDD<Circle, HashSet<Geometry>> joinListResultAfterAggregation = executeDistanceJoinUsingIndex(spatialRDD,queryRDD,considerBoundaryIntersection); 
            JavaPairRDD<Polygon, Long> castedResult = joinListResultAfterAggregation.mapToPair(new PairFunction<Tuple2<Circle,HashSet<Geometry>>,Polygon,Long>()
            {
				@Override
				public Tuple2<Polygon, Long> call(Tuple2<Circle, HashSet<Geometry>> pairObjects) throws Exception {
					HashSet<Polygon> castedSpatialObjects = new HashSet<Polygon>();
					Iterator<Geometry> spatialObjectIterator = pairObjects._2.iterator();
					while(spatialObjectIterator.hasNext())
					{
						Polygon castedSpatialObject = (Polygon)spatialObjectIterator.next();
						castedSpatialObjects.add(castedSpatialObject);
					}
					return new Tuple2<Polygon,Long>((Polygon)pairObjects._1.getCenterGeometry(),(long) castedSpatialObjects.size());
				}
            });
            return castedResult;
        }
        else
        {
        	JavaPairRDD<Circle, HashSet<Geometry>> joinListResultAfterAggregation = executeDistanceJoinNoIndex(spatialRDD,queryRDD,considerBoundaryIntersection); 
        	JavaPairRDD<Polygon, Long> castedResult = joinListResultAfterAggregation.mapToPair(new PairFunction<Tuple2<Circle,HashSet<Geometry>>,Polygon,Long>()
            {
				@Override
				public Tuple2<Polygon, Long> call(Tuple2<Circle, HashSet<Geometry>> pairObjects) throws Exception {
					HashSet<Polygon> castedSpatialObjects = new HashSet<Polygon>();
					Iterator<Geometry> spatialObjectIterator = pairObjects._2.iterator();
					while(spatialObjectIterator.hasNext())
					{
						Polygon castedSpatialObject = (Polygon)spatialObjectIterator.next();
						castedSpatialObjects.add(castedSpatialObject);
					}
					return new Tuple2<Polygon,Long>((Polygon)pairObjects._1.getCenterGeometry(),(long) castedSpatialObjects.size());
				}
            });
            return castedResult;
        }
   }
    

    
 
    
    /**
     * Distance join query count by key.
     *
     * @param spatialRDD the spatial RDD
     * @param queryRDD the query RDD
     * @param useIndex the use index
     * @param considerBoundaryIntersection the consider boundary intersection
     * @return the java pair RDD
     * @throws Exception the exception
     */
    public static JavaPairRDD<Point, Long> DistanceJoinQueryCountByKey(PointRDD spatialRDD,PointRDD queryRDD, boolean useIndex,boolean considerBoundaryIntersection) throws Exception {
        if(useIndex)
        {
        	JavaPairRDD<Circle, HashSet<Geometry>> joinListResultAfterAggregation = executeDistanceJoinUsingIndex(spatialRDD,queryRDD,considerBoundaryIntersection); 
            JavaPairRDD<Point, Long> castedResult = joinListResultAfterAggregation.mapToPair(new PairFunction<Tuple2<Circle,HashSet<Geometry>>,Point,Long>()
            {
				@Override
				public Tuple2<Point, Long> call(Tuple2<Circle, HashSet<Geometry>> pairObjects) throws Exception {
					HashSet<Point> castedSpatialObjects = new HashSet<Point>();
					Iterator<Geometry> spatialObjectIterator = pairObjects._2.iterator();
					while(spatialObjectIterator.hasNext())
					{
						Point castedSpatialObject = (Point)spatialObjectIterator.next();
						castedSpatialObjects.add(castedSpatialObject);
					}
					return new Tuple2<Point,Long>((Point)pairObjects._1.getCenterGeometry(),(long) castedSpatialObjects.size());
				}
            });
            return castedResult;
        }
        else
        {
        	JavaPairRDD<Circle, HashSet<Geometry>> joinListResultAfterAggregation = executeDistanceJoinNoIndex(spatialRDD,queryRDD,considerBoundaryIntersection); 
        	JavaPairRDD<Point, Long> castedResult = joinListResultAfterAggregation.mapToPair(new PairFunction<Tuple2<Circle,HashSet<Geometry>>,Point,Long>()
            {
				@Override
				public Tuple2<Point, Long> call(Tuple2<Circle, HashSet<Geometry>> pairObjects) throws Exception {
					HashSet<Point> castedSpatialObjects = new HashSet<Point>();
					Iterator<Geometry> spatialObjectIterator = pairObjects._2.iterator();
					while(spatialObjectIterator.hasNext())
					{
						Point castedSpatialObject = (Point)spatialObjectIterator.next();
						castedSpatialObjects.add(castedSpatialObject);
					}
					return new Tuple2<Point,Long>((Point)pairObjects._1.getCenterGeometry(),(long) castedSpatialObjects.size());
				}
            });
            return castedResult;
        }
   }
    
    /**
     * Distance join query count by key.
     *
     * @param spatialRDD the spatial RDD
     * @param queryRDD the query RDD
     * @param useIndex the use index
     * @param considerBoundaryIntersection the consider boundary intersection
     * @return the java pair RDD
     * @throws Exception the exception
     */
    public static JavaPairRDD<LineString, Long> DistanceJoinQueryCountByKey(LineStringRDD spatialRDD,LineStringRDD queryRDD, boolean useIndex,boolean considerBoundaryIntersection) throws Exception {
        if(useIndex)
        {
        	JavaPairRDD<Circle, HashSet<Geometry>> joinListResultAfterAggregation = executeDistanceJoinUsingIndex(spatialRDD,queryRDD,considerBoundaryIntersection); 
            JavaPairRDD<LineString, Long> castedResult = joinListResultAfterAggregation.mapToPair(new PairFunction<Tuple2<Circle,HashSet<Geometry>>,LineString,Long>()
            {
				@Override
				public Tuple2<LineString, Long> call(Tuple2<Circle, HashSet<Geometry>> pairObjects) throws Exception {
					HashSet<LineString> castedSpatialObjects = new HashSet<LineString>();
					Iterator<Geometry> spatialObjectIterator = pairObjects._2.iterator();
					while(spatialObjectIterator.hasNext())
					{
						LineString castedSpatialObject = (LineString)spatialObjectIterator.next();
						castedSpatialObjects.add(castedSpatialObject);
					}
					return new Tuple2<LineString,Long>((LineString)pairObjects._1.getCenterGeometry(),(long) castedSpatialObjects.size());
				}
            });
            return castedResult;
        }
        else
        {
        	JavaPairRDD<Circle, HashSet<Geometry>> joinListResultAfterAggregation = executeDistanceJoinNoIndex(spatialRDD,queryRDD,considerBoundaryIntersection); 
        	JavaPairRDD<LineString, Long> castedResult = joinListResultAfterAggregation.mapToPair(new PairFunction<Tuple2<Circle,HashSet<Geometry>>,LineString,Long>()
            {
				@Override
				public Tuple2<LineString, Long> call(Tuple2<Circle, HashSet<Geometry>> pairObjects) throws Exception {
					HashSet<LineString> castedSpatialObjects = new HashSet<LineString>();
					Iterator<Geometry> spatialObjectIterator = pairObjects._2.iterator();
					while(spatialObjectIterator.hasNext())
					{
						LineString castedSpatialObject = (LineString)spatialObjectIterator.next();
						castedSpatialObjects.add(castedSpatialObject);
					}
					return new Tuple2<LineString,Long>((LineString)pairObjects._1.getCenterGeometry(),(long) castedSpatialObjects.size());
				}
            });
            return castedResult;
        }
   }
    
}

