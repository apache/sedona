/**
 * FILE: DistanceJoin.java
 * PATH: org.datasyslab.geospark.spatialOperator.DistanceJoin.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialOperator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.geometryObjects.Circle;
import org.datasyslab.geospark.spatialRDD.CircleRDD;
import org.datasyslab.geospark.spatialRDD.PointRDD;

import com.vividsolutions.jts.geom.Envelope;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.index.SpatialIndex;
import com.vividsolutions.jts.index.quadtree.Quadtree;
import com.vividsolutions.jts.index.strtree.STRtree;

import scala.Tuple2;

// TODO: Auto-generated Javadoc
/**
 * The Class DistanceJoin.
 */
public class DistanceJoin {
	
    /**
     * Spatial join query without index.
     *
     * @param sc the sc
     * @param pointRDD1 the point RDD 1
     * @param pointRDD2 the point RDD 2
     * @param distance the distance
     * @return the java pair RDD
     */
    public static JavaPairRDD<Point, HashSet<Point>> SpatialJoinQueryWithoutIndex(JavaSparkContext sc, PointRDD pointRDD1, PointRDD pointRDD2, Double distance) {
        //Grid filter, Maybe we can filter those key doesn't overlap the destination.

        //Just use grid of Convert pointRDD2 to CircleRDD.
        CircleRDD circleRDD2 = new CircleRDD(pointRDD2, distance);


        final Broadcast<List<Envelope>> envelopeWithGrid = sc.broadcast(pointRDD1.grids);

        JavaPairRDD<Integer, Circle> tmpGridedCircleForQuerySetBeforePartition = circleRDD2.rawSpatialRDD.flatMapToPair(new PairFlatMapFunction<Object, Integer, Circle>() {
            @Override
            public Iterator<Tuple2<Integer, Circle>> call(Object spatialObject) throws Exception {
            	Circle circle = (Circle)spatialObject;
            	HashSet<Tuple2<Integer, Circle>> result = new HashSet<Tuple2<Integer, Circle>>();

            	List<Envelope> grid = envelopeWithGrid.getValue();

                for (int i=0;i<grid.size();i++) {
                    try {
                        if (circle.intersects(grid.get(i))) {
                            result.add(new Tuple2<Integer, Circle>(i, circle));
                        }
                    } catch (NullPointerException exp) {
                        System.out.println(grid.get(i).toString() + circle.toString());
                    }
                }
                return result.iterator();

            }
        });

        JavaPairRDD<Integer, Circle> tmpGridRDDForQuerySet = tmpGridedCircleForQuerySetBeforePartition.partitionBy(pointRDD1.spatialPartitionedRDD.partitioner().get()).persist(StorageLevel.DISK_ONLY());

        JavaPairRDD<Integer, Tuple2<Iterable<Object>, Iterable<Circle>>> cogroupResult = pointRDD1.spatialPartitionedRDD.cogroup(tmpGridRDDForQuerySet);

        JavaPairRDD<Object, HashSet<Object>> joinResultBeforeAggregation = cogroupResult.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<Object>, Iterable<Circle>>>, Object, HashSet<Object>>() {
            @Override
            public Iterator<Tuple2<Object, HashSet<Object>>> call(Tuple2<Integer, Tuple2<Iterable<Object>, Iterable<Circle>>> cogroup) throws Exception {
            	HashSet<Tuple2<Object, HashSet<Object>>> result = new HashSet<Tuple2<Object, HashSet<Object>>>();

                Tuple2<Iterable<Object>, Iterable<Circle>> cogroupTupleList = cogroup._2();
                HashSet<Object> points = new HashSet<Object>();
                for (Object p : cogroupTupleList._1()) {
                    points.add((Point)p);
                    ;
                }
                for (Circle c : cogroupTupleList._2()) {
                    HashSet<Object> poinitHashSet = new HashSet<Object>();
                    //Since it is iterable not arrayList, Is it possible when it runs to the end, it will not goes back?
                    for (Object p : points) {
                        if (c.contains((Point)p)) {
                            poinitHashSet.add((Point)p);
                        }
                    }
                    result.add(new Tuple2<Object, HashSet<Object>>(c.getCenter(), poinitHashSet));
                }
                return result.iterator();
            }
        });


        //AggregateByKey?
        JavaPairRDD<Object, HashSet<Object>> joinResultAfterAggregation = joinResultBeforeAggregation.reduceByKey(new Function2<HashSet<Object>, HashSet<Object>, HashSet<Object>>() {
            @Override
            public HashSet<Object> call(HashSet<Object> points, HashSet<Object> points2) throws Exception {
                points.addAll(points2);
                return points;
            }
        });

        JavaPairRDD<Object, HashSet<Object>> joinListResultAfterAggregation = joinResultAfterAggregation.mapValues(new Function<HashSet<Object>, HashSet<Object>>() {
            @Override
            public HashSet<Object> call(HashSet<Object> points) throws Exception {
                return new HashSet<Object>(points);
            }
        });
        JavaPairRDD<Point, HashSet<Point>> castJoinListResultAfterAggregation = joinListResultAfterAggregation.mapToPair(new PairFunction<Tuple2<Object,HashSet<Object>>,Point,HashSet<Point>>()
        {

			@Override
			public Tuple2<Point, HashSet<Point>> call(Tuple2<Object, HashSet<Object>> t) throws Exception {
				Point firstSpatialObject = (Point) t._1();
				HashSet<Point> secondSpatialObjects = new HashSet<Point>();
				Iterator iterator = t._2().iterator();
				while(iterator.hasNext())
				{
					secondSpatialObjects.add((Point)iterator.next());
				}
				return new Tuple2(firstSpatialObject,secondSpatialObjects);
			}
        	
        });
        return castJoinListResultAfterAggregation;
    }


    /**
     * Spatial join query using index.
     *
     * @param sc the sc
     * @param pointRDD1 the point RDD 1
     * @param pointRDD2 the point RDD 2
     * @param distance the distance
     * @return the java pair RDD
     */
    public static JavaPairRDD<Point, List<Point>> SpatialJoinQueryUsingIndex(JavaSparkContext sc, PointRDD pointRDD1, PointRDD pointRDD2, Double distance) {
        //Grid filter, Maybe we can filter those key doesn't overlap the destination.

        //Just use grid of Convert pointRDD2 to CircleRDD.
        CircleRDD circleRDD2 = new CircleRDD(pointRDD2, distance);

        //Build grid on circleRDD2.

        final Broadcast<List<Envelope>> envelopeWithGrid = sc.broadcast(pointRDD1.grids);

        JavaPairRDD<Integer, Circle> tmpGridedCircleForQuerySetBeforePartition = circleRDD2.rawSpatialRDD.flatMapToPair(new PairFlatMapFunction<Object, Integer, Circle>() {
            @Override
            public Iterator<Tuple2<Integer, Circle>> call(Object spatialObject) throws Exception {
            	Circle circle = (Circle) spatialObject;
            	HashSet<Tuple2<Integer, Circle>> result = new HashSet<Tuple2<Integer, Circle>>();
            	List<Envelope> grid = envelopeWithGrid.getValue();

                for (int i=0;i<grid.size();i++) {
                    try {
                        if (circle.intersects(grid.get(i))) {
                            result.add(new Tuple2<Integer, Circle>(i, circle));
                        }
                    } catch (NullPointerException exp) {
                        System.out.println(grid.get(i).toString() + circle.toString());
                    }
                }
                return result.iterator();

            }
        });

        JavaPairRDD<Integer, Circle> tmpGridRDDForQuerySet = tmpGridedCircleForQuerySetBeforePartition.partitionBy(pointRDD1.spatialPartitionedRDD.partitioner().get()).persist(StorageLevel.DISK_ONLY());

        JavaPairRDD<Integer, Tuple2<Iterable<Object>, Iterable<Circle>>> cogroupResult = pointRDD1.indexedRDD.cogroup(tmpGridRDDForQuerySet);

        JavaPairRDD<Point, HashSet<Point>> joinResultBeforeAggregation = cogroupResult.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<Object>, Iterable<Circle>>>, Point, HashSet<Point>>() {
            @Override
            public Iterator<Tuple2<Point, HashSet<Point>>> call(Tuple2<Integer, Tuple2<Iterable<Object>, Iterable<Circle>>> cogroup) throws Exception {
            	HashSet<Tuple2<Point, HashSet<Point>>> result = new HashSet<Tuple2<Point, HashSet<Point>>>();
                SpatialIndex treeIndex=(SpatialIndex) cogroup._2()._1().iterator().next();
                if(treeIndex instanceof STRtree)
                {
                	treeIndex = (STRtree)treeIndex;
                }
                else
                {
                	treeIndex = (Quadtree)treeIndex;
                }
                for (Object c : cogroup._2()._2()) {
                    List<Point> pointList = new ArrayList<Point>();

                    pointList = treeIndex.query(((Circle)c).getMBR());
                    HashSet<Point> pointSet = new HashSet<Point>(pointList);
                    result.add(new Tuple2<Point, HashSet<Point>>(((Circle)c).getCenter(), pointSet));
                }
                return result.iterator();
            }

        });


        //AggregateByKey?
        JavaPairRDD<Point, HashSet<Point>> joinResultAfterAggregation = joinResultBeforeAggregation.reduceByKey(new Function2<HashSet<Point>, HashSet<Point>, HashSet<Point>>() {
            @Override
            public HashSet<Point> call(HashSet<Point> points, HashSet<Point> points2) throws Exception {
                points.addAll(points2);
                return points;
            }
        });

        JavaPairRDD<Point, List<Point>> joinListResultAfterAggregation = joinResultAfterAggregation.mapValues(new Function<HashSet<Point>, List<Point>>() {
            @Override
            public List<Point> call(HashSet<Point> points) throws Exception {
                return new ArrayList<Point>(points);
            }
        });

        return joinListResultAfterAggregation;
    }

}
