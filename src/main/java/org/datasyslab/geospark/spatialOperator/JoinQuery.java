package org.datasyslab.geospark.spatialOperator;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.strtree.STRtree;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.geometryObjects.EnvelopeWithGrid;
import org.datasyslab.geospark.joinJudgement.PointByPolygonJudgement;
import org.datasyslab.geospark.joinJudgement.PointByPolygonJudgementUsingIndex;
import org.datasyslab.geospark.joinJudgement.PointByRectangleJudgement;
import org.datasyslab.geospark.joinJudgement.PointByRectangleJudgementUsingIndex;
import org.datasyslab.geospark.joinJudgement.PolygonByPolygonJudgement;
import org.datasyslab.geospark.joinJudgement.PolygonByPolygonJudgementUsingIndex;
import org.datasyslab.geospark.joinJudgement.RectangleByRectangleJudgement;
import org.datasyslab.geospark.joinJudgement.RectangleByRectangleJudgementUsingIndex;
import org.datasyslab.geospark.spatialPartitioning.MissingObjectsHandler;
import org.datasyslab.geospark.spatialPartitioning.PartitionJudgement;
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import scala.Tuple2;

//todo: Replace older join query class.
/**
 * @author sparkadmin
 *
 */
public class JoinQuery implements Serializable{

	public PolygonRDD polygonRDD;
	public RectangleRDD rectangleRDD;
	double distance=0.0;
	JavaSparkContext sc;
	public JoinQuery(JavaSparkContext sc,PointRDD pointRDD, RectangleRDD rectangleRDDUnpartitioned)
	{
        if(pointRDD.gridPointRDD == null) {
            throw new NullPointerException("Need to do spatial partitioning first, gridedSRDD is null");
        }
		this.rectangleRDD=rectangleRDDUnpartitioned;
		this.rectangleRDD.rawRectangleRDD=rectangleRDDUnpartitioned.rawRectangleRDD;
		this.rectangleRDD.SpatialPartition(pointRDD.grids);
		this.sc=sc;
		//this.rectangleRDD.gridRectangleRDD.persist(StorageLevel.MEMORY_ONLY());
	}
	public JoinQuery(JavaSparkContext sc,RectangleRDD objectRDD, RectangleRDD rectangleRDDUnpartitioned)
	{
        if(objectRDD.gridRectangleRDD == null) {
            throw new NullPointerException("Need to do spatial partitioning first, gridedSRDD is null");
        }
		this.rectangleRDD=rectangleRDDUnpartitioned;
		this.rectangleRDD.rawRectangleRDD=rectangleRDDUnpartitioned.rawRectangleRDD;
		this.rectangleRDD.SpatialPartition(objectRDD.grids);
		this.sc=sc;
	}
	public JoinQuery(JavaSparkContext sc,PointRDD pointRDD, PolygonRDD polygonRDDUnpartitioned)
	{
        if(pointRDD.gridPointRDD == null) {
            throw new NullPointerException("Need to do spatial partitioning first, gridedSRDD is null");
        }
		this.polygonRDD=polygonRDDUnpartitioned;
		this.polygonRDD.rawPolygonRDD=polygonRDDUnpartitioned.rawPolygonRDD;
		this.polygonRDD.SpatialPartition(pointRDD.grids);
		this.sc=sc;
	}

	public JoinQuery(JavaSparkContext sc,PolygonRDD objectRDD, PolygonRDD polygonRDDUnpartitioned)
	{
        if(objectRDD.gridPolygonRDD == null) {
            throw new NullPointerException("Need to do spatial partitioning first, gridedSRDD is null");
        }
		this.polygonRDD=polygonRDDUnpartitioned;
		this.polygonRDD.rawPolygonRDD=polygonRDDUnpartitioned.rawPolygonRDD;
		this.polygonRDD.SpatialPartition(objectRDD.grids);
		this.sc=sc;
	}

    /**
     * Spatial Join Query between a RectangleRDD and a PointRDD using index nested loop. The PointRDD should be indexed in advance.
     * @param sc SparkContext which defines some Spark configurations
     * @param pointRDD Indexed PointRDD
     * @param rectangleRDD RectangleRDD
     * @return A PairRDD which follows the schema: Envelope, A list of points covered by this envelope
     */
    public JavaPairRDD<Envelope, HashSet<Point>> SpatialJoinQueryUsingIndex(PointRDD pointRDD,RectangleRDD rectangleRDD) {

        //Check if rawPointRDD have index.
        if(pointRDD.indexedRDD == null) {
            throw new NullPointerException("Need to invoke buildIndex() first, indexedRDD is null");
        }
        if(pointRDD.gridPointRDD == null) {
            throw new NullPointerException("Need to do spatial partitioning first, gridedSRDD is null");
        }
        //JavaPairRDD<Integer, Envelope> tmpGridRDDForQuerySet = getIntegerEnvelopeJavaPairRDD(pointRDD, rectangleRDD);
      //Handle missing objects by spatial partitioning
        MissingObjectsHandler handler=new MissingObjectsHandler();
        JavaPairRDD<Envelope, HashSet<Point>> missingObjectResult=handler.handleMissingObjectsUsingIndex(sc,pointRDD, rectangleRDD);
        
        JavaPairRDD<Integer, Tuple2<Iterable<STRtree>, Iterable<Envelope>>> cogroupResult = pointRDD.indexedRDD.cogroup(this.rectangleRDD.gridRectangleRDD);

        //flatMapToPair, use HashSet.
        //This will be really time consuiming.. But the memory usage will be less then previous solution.
        //todo: Verify this implementation reduce shuffle???
        JavaPairRDD<Envelope, HashSet<Point>> joinResultMissingResults = cogroupResult.flatMapToPair(new PointByRectangleJudgementUsingIndex(pointRDD.grids.size()));
        JavaPairRDD<Envelope, HashSet<Point>> joinResultBeforeAggregation=joinResultMissingResults.union(missingObjectResult);
        //AggregateByKey?
        JavaPairRDD<Envelope, HashSet<Point>> joinListResultAfterAggregation = aggregateJoinResultPointByRectangle(joinResultBeforeAggregation);

        return joinListResultAfterAggregation;
    }

    
    /**
     * Spatial Join Query between a RectangleRDD and a PointRDD using regular nested loop. The PointRDD should be indexed in advance.
     * @param sc SparkContext which defines some Spark configurations
     * @param pointRDD Indexed PointRDD
     * @param rectangleRDD RectangleRDD
     * @param cacheTmpGrid Deprecated . Use "true"
     * @return A PairRDD which follows the schema: Envelope, A list of points covered by this envelope
     */
    public JavaPairRDD<Envelope, HashSet<Point>> SpatialJoinQuery(PointRDD pointRDD,RectangleRDD rectangleRDD) {
        //todo: Add logic, if this is cached, no need to calculate it again.
       // JavaPairRDD<Integer, Envelope> tmpGridRDDForQuerySet = getIntegerEnvelopeJavaPairRDD( pointRDD, rectangleRDD);

    	
    	
        if(pointRDD.gridPointRDD == null) {
            throw new NullPointerException("Need to do spatial partitioning first, gridedSRDD is null");
        }
        //Handle missing objects by spatial partitioning
        MissingObjectsHandler handler=new MissingObjectsHandler();
        JavaPairRDD<Envelope, HashSet<Point>> missingObjectResult=handler.handleMissingObjects(sc,pointRDD, rectangleRDD);
        
        JavaPairRDD<Integer, Tuple2<Iterable<Point>, Iterable<Envelope>>> cogroupResult = pointRDD.gridPointRDD.cogroup(this.rectangleRDD.gridRectangleRDD);
            

            //flatMapToPair, use HashSet.
            //This will be really time consuiming.. But the memory usage will be less then in version 1.0
            //todo: Verify this implementation will reduce shuffle
            
            JavaPairRDD<Envelope, HashSet<Point>> joinResultMissingResults = cogroupResult.flatMapToPair(new PointByRectangleJudgement(pointRDD.grids.size()));
            
            JavaPairRDD<Envelope, HashSet<Point>> joinResultBeforeAggregation=joinResultMissingResults.union(missingObjectResult);
            JavaPairRDD<Envelope, HashSet<Point>> joinListResultAfterAggregation = aggregateJoinResultPointByRectangle(joinResultBeforeAggregation);
            return joinListResultAfterAggregation;
            //Conver HashSet to a better way? May ArrayList,

    }
    
 

    /**
     * @param objectRDD should be indexed and spatial-partitioned first
     * @param rectangleRDD
     * @return
     */
    public JavaPairRDD<Envelope, HashSet<Envelope>> SpatialJoinQueryUsingIndex(RectangleRDD objectRDD,RectangleRDD rectangleRDD) {

        //Check if rawPointRDD have index.
        if(objectRDD.indexedRDD == null) {
            throw new NullPointerException("Need to invoke buildIndex() first, indexedRDD is null");
        }
        if(objectRDD.gridRectangleRDD == null) {
            throw new NullPointerException("Need to do spatial partitioning first, gridedSRDD is null");
        }
        //JavaPairRDD<Integer, Envelope> tmpGridRDDForQuerySet = getIntegerEnvelopeJavaPairRDD(pointRDD, rectangleRDD);

        //Handle missing objects by spatial partitioning
        MissingObjectsHandler handler=new MissingObjectsHandler();
        JavaPairRDD<Envelope, HashSet<Envelope>> missingObjectResult=handler.handleMissingObjectsUsingIndex(sc,objectRDD, rectangleRDD);
        
        JavaPairRDD<Integer, Tuple2<Iterable<STRtree>, Iterable<Envelope>>> cogroupResult = objectRDD.indexedRDD.cogroup(this.rectangleRDD.gridRectangleRDD);

        //flatMapToPair, use HashSet.
        //This will be really time consuiming.. But the memory usage will be less then previous solution.
        //todo: Verify this implementation reduce shuffle???
        JavaPairRDD<Envelope, HashSet<Envelope>> joinResultMissingResults = cogroupResult.flatMapToPair(new RectangleByRectangleJudgementUsingIndex(objectRDD.grids.size()));
        JavaPairRDD<Envelope, HashSet<Envelope>> joinResultBeforeAggregation=joinResultMissingResults.union(missingObjectResult);
        //AggregateByKey?
        JavaPairRDD<Envelope, HashSet<Envelope>> joinListResultAfterAggregation = aggregateJoinResultRectangleByRectangle(joinResultBeforeAggregation);

        return joinListResultAfterAggregation;
    }

    

    /**
     * @param objectRDD should be spatial partitioned first
     * @param rectangleRDD
     * @return
     */
    public JavaPairRDD<Envelope, HashSet<Envelope>> SpatialJoinQuery(RectangleRDD objectRDD,RectangleRDD rectangleRDD) {
        //todo: Add logic, if this is cached, no need to calculate it again.
       // JavaPairRDD<Integer, Envelope> tmpGridRDDForQuerySet = getIntegerEnvelopeJavaPairRDD( pointRDD, rectangleRDD);

        //cogroup
    	
        if(objectRDD.gridRectangleRDD == null) {
            throw new NullPointerException("Need to do spatial partitioning first, gridedSRDD is null");
        }
        	//Handle missing objects by spatial partitioning
        	MissingObjectsHandler handler=new MissingObjectsHandler();
        	JavaPairRDD<Envelope, HashSet<Envelope>> missingObjectResult=handler.handleMissingObjects(sc,objectRDD, rectangleRDD);
        
            JavaPairRDD<Integer, Tuple2<Iterable<Envelope>, Iterable<Envelope>>> cogroupResult = objectRDD.gridRectangleRDD.cogroup(this.rectangleRDD.gridRectangleRDD);


            //flatMapToPair, use HashSet.
            //This will be really time consuiming.. But the memory usage will be less then in version 1.0
            //todo: Verify this implementation will reduce shuffle
            JavaPairRDD<Envelope, HashSet<Envelope>> joinResultMissingResults = cogroupResult.flatMapToPair(new RectangleByRectangleJudgement(objectRDD.grids.size()));
            JavaPairRDD<Envelope, HashSet<Envelope>> joinResultBeforeAggregation=joinResultMissingResults.union(missingObjectResult);
            JavaPairRDD<Envelope, HashSet<Envelope>> joinListResultAfterAggregation = aggregateJoinResultRectangleByRectangle(joinResultBeforeAggregation);
            return joinListResultAfterAggregation;
            //Conver HashSet to a better way? May ArrayList,

    }
    

   /**
 * @param pointRDD should be spatial partitioned first
 * @param polygonRDD
 * @return
 */
public JavaPairRDD<Polygon, HashSet<Point>> SpatialJoinQuery(PointRDD pointRDD,PolygonRDD polygonRDD) {
       //todo: Add logic, if this is cached, no need to calculate it again.
      // JavaPairRDD<Integer, Envelope> tmpGridRDDForQuerySet = getIntegerEnvelopeJavaPairRDD( pointRDD, rectangleRDD);

       //cogroup
   	
       if(pointRDD.gridPointRDD == null) {
           throw new NullPointerException("Need to do spatial partitioning first, gridedSRDD is null");
       }

           JavaPairRDD<Integer, Tuple2<Iterable<Point>, Iterable<Polygon>>> cogroupResult = pointRDD.gridPointRDD.cogroup(this.polygonRDD.gridPolygonRDD);


           //flatMapToPair, use HashSet.
           //This will be really time consuiming.. But the memory usage will be less then in version 1.0
           //todo: Verify this implementation will reduce shuffle
           JavaPairRDD<Polygon, HashSet<Point>> joinResultBeforeAggregation = cogroupResult.flatMapToPair(new PointByPolygonJudgement());
           JavaPairRDD<Polygon, HashSet<Point>> joinListResultAfterAggregation = aggregateJoinResultPointByPolygon(joinResultBeforeAggregation);
           return joinListResultAfterAggregation;
           //Conver HashSet to a better way? May ArrayList,

   }


   /**
 * @param pointRDD should be indexed and spatial-partitioned in advance.
 * @param polygonRDD
 * @return
 */
public JavaPairRDD<Polygon, HashSet<Point>> SpatialJoinQueryUsingIndex(PointRDD pointRDD,PolygonRDD polygonRDD) {

       //Check if rawPointRDD have index.
       if(pointRDD.indexedRDD == null) {
           throw new NullPointerException("Need to invoke buildIndex() first, indexedRDD is null");
       }
       if(pointRDD.gridPointRDD == null) {
           throw new NullPointerException("Need to do spatial partitioning first, gridedSRDD is null");
       }
       //JavaPairRDD<Integer, Envelope> tmpGridRDDForQuerySet = getIntegerEnvelopeJavaPairRDD(pointRDD, rectangleRDD);


       JavaPairRDD<Integer, Tuple2<Iterable<STRtree>, Iterable<Polygon>>> cogroupResult = pointRDD.indexedRDD.cogroup(this.polygonRDD.gridPolygonRDD);

       //flatMapToPair, use HashSet.
       //This will be really time consuiming.. But the memory usage will be less then previous solution.
       //todo: Verify this implementation reduce shuffle???
       JavaPairRDD<Polygon, HashSet<Point>> joinResultBeforeAggregation = cogroupResult.flatMapToPair(new PointByPolygonJudgementUsingIndex());

       //AggregateByKey?
       JavaPairRDD<Polygon, HashSet<Point>> joinListResultAfterAggregation = aggregateJoinResultPointByPolygon(joinResultBeforeAggregation);

       return joinListResultAfterAggregation;
   }
   

   /**
 * @param objectRDD should be indexed and spatial-partitioned first
 * @param windowRDD
 * @return
 */
public JavaPairRDD<Polygon, HashSet<Polygon>> SpatialJoinQueryUsingIndex(PolygonRDD objectRDD,PolygonRDD windowRDD) {

       //Check if rawPointRDD have index.
       if(objectRDD.indexedRDD == null) {
           throw new NullPointerException("Need to invoke buildIndex() first, indexedRDD is null");
       }
       if(objectRDD.gridPolygonRDD == null) {
           throw new NullPointerException("Need to do spatial partitioning first, gridedSRDD is null");
       }
       //JavaPairRDD<Integer, Envelope> tmpGridRDDForQuerySet = getIntegerEnvelopeJavaPairRDD(pointRDD, rectangleRDD);

       //Handle missing objects by spatial partitioning
       MissingObjectsHandler handler=new MissingObjectsHandler();
       JavaPairRDD<Polygon, HashSet<Polygon>> missingObjectResult=handler.handleMissingObjectsUsingIndex(sc,objectRDD, windowRDD);
       
       JavaPairRDD<Integer, Tuple2<Iterable<STRtree>, Iterable<Polygon>>> cogroupResult = objectRDD.indexedRDD.cogroup(this.polygonRDD.gridPolygonRDD);

       //flatMapToPair, use HashSet.
       //This will be really time consuiming.. But the memory usage will be less then previous solution.
       //todo: Verify this implementation reduce shuffle???
       JavaPairRDD<Polygon, HashSet<Polygon>> joinResultMissingResults = cogroupResult.flatMapToPair(new PolygonByPolygonJudgementUsingIndex(objectRDD.grids.size()));
       JavaPairRDD<Polygon, HashSet<Polygon>> joinResultBeforeAggregation=joinResultMissingResults.union(missingObjectResult);
       //AggregateByKey?
       JavaPairRDD<Polygon, HashSet<Polygon>> joinListResultAfterAggregation = aggregateJoinResultPolygonByPolygon(joinResultBeforeAggregation);

       return joinListResultAfterAggregation;
   }


   /**
 * @param objectRDD should be spatial partitioned first
 * @param polygonRDD
 * @return
 */
public JavaPairRDD<Polygon, HashSet<Polygon>> SpatialJoinQuery(PolygonRDD objectRDD,PolygonRDD polygonRDD) {
       //todo: Add logic, if this is cached, no need to calculate it again.
      // JavaPairRDD<Integer, Envelope> tmpGridRDDForQuerySet = getIntegerEnvelopeJavaPairRDD( pointRDD, rectangleRDD);

       //cogroup
   	
       if(objectRDD.gridPolygonRDD == null) {
           throw new NullPointerException("Need to do spatial partitioning first, gridedSRDD is null");
       }
       	//Handle missing objects by spatial partitioning
       	MissingObjectsHandler handler=new MissingObjectsHandler();
       	JavaPairRDD<Polygon, HashSet<Polygon>> missingObjectResult=handler.handleMissingObjects(sc,objectRDD, polygonRDD);
       
           JavaPairRDD<Integer, Tuple2<Iterable<Polygon>, Iterable<Polygon>>> cogroupResult = objectRDD.gridPolygonRDD.cogroup(this.polygonRDD.gridPolygonRDD);


           //flatMapToPair, use HashSet.
           //This will be really time consuiming.. But the memory usage will be less then in version 1.0
           //todo: Verify this implementation will reduce shuffle
           JavaPairRDD<Polygon, HashSet<Polygon>> joinResultMissingResults = cogroupResult.flatMapToPair(new PolygonByPolygonJudgement(objectRDD.grids.size()));
           JavaPairRDD<Polygon, HashSet<Polygon>> joinResultBeforeAggregation=joinResultMissingResults.union(missingObjectResult);
           JavaPairRDD<Polygon, HashSet<Polygon>> joinListResultAfterAggregation = aggregateJoinResultPolygonByPolygon(joinResultBeforeAggregation);
           return joinListResultAfterAggregation;
           //Conver HashSet to a better way? May ArrayList,

   }  

   
   
    private static JavaPairRDD<Envelope, HashSet<Point>> aggregateJoinResultPointByRectangle(JavaPairRDD<Envelope, HashSet<Point>> joinResultBeforeAggregation) {
        //AggregateByKey?
        JavaPairRDD<Envelope, HashSet<Point>> joinResultAfterAggregation = joinResultBeforeAggregation.reduceByKey(new Function2<HashSet<Point>, HashSet<Point>, HashSet<Point>>() {
            @Override
            public HashSet<Point> call(HashSet<Point> points, HashSet<Point> points2) throws Exception {
                points.addAll(points2);
                return points;
            }
        });
     
        return joinResultAfterAggregation.mapValues(new Function<HashSet<Point>, HashSet<Point>>() {
            @Override
            public HashSet<Point> call(HashSet<Point> points) throws Exception {
                return new HashSet<Point>(points);
            }
        });
    }
    private static JavaPairRDD<Envelope, HashSet<Envelope>> aggregateJoinResultRectangleByRectangle(JavaPairRDD<Envelope, HashSet<Envelope>> joinResultBeforeAggregation) {
        //AggregateByKey?
        JavaPairRDD<Envelope, HashSet<Envelope>> joinResultAfterAggregation = joinResultBeforeAggregation.reduceByKey(new Function2<HashSet<Envelope>, HashSet<Envelope>, HashSet<Envelope>>() {
            @Override
            public HashSet<Envelope> call(HashSet<Envelope> objects, HashSet<Envelope> objects2) throws Exception {
            	objects.addAll(objects2);
                return objects;
            }
        });
     
        return joinResultAfterAggregation.mapValues(new Function<HashSet<Envelope>, HashSet<Envelope>>() {
            @Override
            public HashSet<Envelope> call(HashSet<Envelope> objects) throws Exception {
                return new HashSet<Envelope>(objects);
            }
        });
    }
    private static JavaPairRDD<Polygon, HashSet<Point>> aggregateJoinResultPointByPolygon(JavaPairRDD<Polygon, HashSet<Point>> joinResultBeforeAggregation) {
            //AggregateByKey?
            JavaPairRDD<Polygon, HashSet<Point>> joinResultAfterAggregation = joinResultBeforeAggregation.reduceByKey(new Function2<HashSet<Point>, HashSet<Point>, HashSet<Point>>() {
                @Override
                public HashSet<Point> call(HashSet<Point> points, HashSet<Point> points2) throws Exception {
                    points.addAll(points2);
                    return points;
                }
            });
        return joinResultAfterAggregation.mapValues(new Function<HashSet<Point>, HashSet<Point>>() {
            @Override
            public HashSet<Point> call(HashSet<Point> points) throws Exception {
                return new HashSet<Point>(points);
            }
        });
    }
    private static JavaPairRDD<Polygon, HashSet<Polygon>> aggregateJoinResultPolygonByPolygon(JavaPairRDD<Polygon, HashSet<Polygon>> joinResultBeforeAggregation) {
            //AggregateByKey?
            JavaPairRDD<Polygon, HashSet<Polygon>> joinResultAfterAggregation = joinResultBeforeAggregation.reduceByKey(new Function2<HashSet<Polygon>, HashSet<Polygon>, HashSet<Polygon>>() {
                @Override
                public HashSet<Polygon> call(HashSet<Polygon> objects, HashSet<Polygon> objects2) throws Exception {
                	objects.addAll(objects2);
                    return objects;
                }
            });
         
            return joinResultAfterAggregation.mapValues(new Function<HashSet<Polygon>, HashSet<Polygon>>() {
                @Override
                public HashSet<Polygon> call(HashSet<Polygon> objects) throws Exception {
                    return new HashSet<Polygon>(objects);
                }
            });
        }




}
