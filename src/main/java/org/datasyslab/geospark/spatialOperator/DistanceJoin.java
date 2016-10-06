package org.datasyslab.geospark.spatialOperator;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.index.strtree.STRtree;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.geometryObjects.Circle;
import org.datasyslab.geospark.geometryObjects.EnvelopeWithGrid;
import org.datasyslab.geospark.spatialRDD.CircleRDD;
import org.datasyslab.geospark.spatialRDD.PointRDD;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import scala.Tuple2;

public class DistanceJoin {
	
    /**
     * Deprecated. This method is to be updated for using index.
     * Spatial Join Query between two PointRDDs using regular nested loop. For each point in PointRDD2, it will find the points in PointRDD1 which are within the specified distance of this point.
     * @param sc SparkContext which defines some Spark configurations
     * @param pointRDD1 PointRDD1
     * @param pointRDD2 PointRDD2
     * @param distance specify the distance predicate between two points
     * @return A PairRDD which follows the schema: Point in PoitRDD2, a list of qualified points in PointRDD1
     */
    public static JavaPairRDD<Point, HashSet<Point>> SpatialJoinQueryWithoutIndex(JavaSparkContext sc, PointRDD pointRDD1, PointRDD pointRDD2, Double distance) {
        //Grid filter, Maybe we can filter those key doesn't overlap the destination.

        //Just use grid of Convert pointRDD2 to CircleRDD.
        CircleRDD circleRDD2 = new CircleRDD(pointRDD2, distance);


        final Broadcast<HashSet<EnvelopeWithGrid>> envelopeWithGrid = sc.broadcast(pointRDD1.grids);

        JavaPairRDD<Integer, Circle> tmpGridedCircleForQuerySetBeforePartition = circleRDD2.getCircleRDD().flatMapToPair(new PairFlatMapFunction<Circle, Integer, Circle>() {
            @Override
            public Iterator<Tuple2<Integer, Circle>> call(Circle circle) throws Exception {
            	HashSet<Tuple2<Integer, Circle>> result = new HashSet<Tuple2<Integer, Circle>>();

            	HashSet<EnvelopeWithGrid> grid = envelopeWithGrid.getValue();

                for (EnvelopeWithGrid e : grid) {
                    try {
                        if (circle.intersects(e)) {
                            result.add(new Tuple2<Integer, Circle>(e.grid, circle));
                        }
                    } catch (NullPointerException exp) {
                        System.out.println(e.toString() + circle.toString());
                    }
                }
                return result.iterator();

            }
        });

        JavaPairRDD<Integer, Circle> tmpGridRDDForQuerySet = tmpGridedCircleForQuerySetBeforePartition.partitionBy(pointRDD1.gridPointRDD.partitioner().get()).persist(StorageLevel.DISK_ONLY());

        JavaPairRDD<Integer, Tuple2<Iterable<Point>, Iterable<Circle>>> cogroupResult = pointRDD1.gridPointRDD.cogroup(tmpGridRDDForQuerySet);

        JavaPairRDD<Point, HashSet<Point>> joinResultBeforeAggregation = cogroupResult.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<Point>, Iterable<Circle>>>, Point, HashSet<Point>>() {
            @Override
            public Iterator<Tuple2<Point, HashSet<Point>>> call(Tuple2<Integer, Tuple2<Iterable<Point>, Iterable<Circle>>> cogroup) throws Exception {
            	HashSet<Tuple2<Point, HashSet<Point>>> result = new HashSet<Tuple2<Point, HashSet<Point>>>();

                Tuple2<Iterable<Point>, Iterable<Circle>> cogroupTupleList = cogroup._2();
                HashSet<Point> points = new HashSet<Point>();
                for (Point p : cogroupTupleList._1()) {
                    points.add(p);
                    ;
                }
                for (Circle c : cogroupTupleList._2()) {
                    HashSet<Point> poinitHashSet = new HashSet<Point>();
                    //Since it is iterable not arrayList, Is it possible when it runs to the end, it will not goes back?
                    for (Point p : points) {
                        if (c.contains(p)) {
                            poinitHashSet.add(p);
                        }
                    }
                    result.add(new Tuple2<Point, HashSet<Point>>(c.getCenter(), poinitHashSet));
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

        JavaPairRDD<Point, HashSet<Point>> joinListResultAfterAggregation = joinResultAfterAggregation.mapValues(new Function<HashSet<Point>, HashSet<Point>>() {
            @Override
            public HashSet<Point> call(HashSet<Point> points) throws Exception {
                return new HashSet<Point>(points);
            }
        });

        return joinListResultAfterAggregation;
    }


    /**
     * Spatial Join Query between two PointRDDs using index nested loop. PointRDD1 should be indexed in advance. For each point in PointRDD2, it will find the points in PointRDD1 which are within the specified distance of this point.
     * @param sc SparkContext which defines some Spark configurations
     * @param pointRDD1 PointRDD1
     * @param pointRDD2 PointRDD2
     * @param distance specify the distance predicate between two points
     * @return A PairRDD which follows the schema: Point in PoitRDD2, a list of qualified points in PointRDD1
     */
    public static JavaPairRDD<Point, List<Point>> SpatialJoinQueryUsingIndex(JavaSparkContext sc, PointRDD pointRDD1, PointRDD pointRDD2, Double distance) {
        //Grid filter, Maybe we can filter those key doesn't overlap the destination.

        //Just use grid of Convert pointRDD2 to CircleRDD.
        CircleRDD circleRDD2 = new CircleRDD(pointRDD2, distance);

        //Build grid on circleRDD2.

        final Broadcast<HashSet<EnvelopeWithGrid>> envelopeWithGrid = sc.broadcast(pointRDD1.grids);

        JavaPairRDD<Integer, Circle> tmpGridedCircleForQuerySetBeforePartition = circleRDD2.getCircleRDD().flatMapToPair(new PairFlatMapFunction<Circle, Integer, Circle>() {
            @Override
            public Iterator<Tuple2<Integer, Circle>> call(Circle circle) throws Exception {
            	HashSet<Tuple2<Integer, Circle>> result = new HashSet<Tuple2<Integer, Circle>>();

                HashSet<EnvelopeWithGrid> grid = envelopeWithGrid.getValue();

                for (EnvelopeWithGrid e : grid) {
                    if (circle.intersects(e)) {
                        result.add(new Tuple2<Integer, Circle>(e.grid, circle));
                    }
                }
                return result.iterator();

            }
        });

        JavaPairRDD<Integer, Circle> tmpGridRDDForQuerySet = tmpGridedCircleForQuerySetBeforePartition.partitionBy(pointRDD1.gridPointRDD.partitioner().get()).persist(StorageLevel.DISK_ONLY());

        JavaPairRDD<Integer, Tuple2<Iterable<STRtree>, Iterable<Circle>>> cogroupResult = pointRDD1.indexedRDD.cogroup(tmpGridRDDForQuerySet);

        JavaPairRDD<Point, HashSet<Point>> joinResultBeforeAggregation = cogroupResult.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<STRtree>, Iterable<Circle>>>, Point, HashSet<Point>>() {
            @Override
            public Iterator<Tuple2<Point, HashSet<Point>>> call(Tuple2<Integer, Tuple2<Iterable<STRtree>, Iterable<Circle>>> cogroup) throws Exception {
            	HashSet<Tuple2<Point, HashSet<Point>>> result = new HashSet<Tuple2<Point, HashSet<Point>>>();

                Tuple2<Iterable<STRtree>, Iterable<Circle>> cogroupTupleList = cogroup._2();
                for (Circle c : cogroupTupleList._2()) {
                    List<Point> pointList = new ArrayList<Point>();
                    for (STRtree s : cogroupTupleList._1()) {
                        //这可以? 他都不知道类型把..
                        pointList = s.query(c.getMBR());

                        //This is not enough, need to verify again.

                    }
                    HashSet<Point> pointSet = new HashSet<Point>(pointList);
                    result.add(new Tuple2<Point, HashSet<Point>>(c.getCenter(), pointSet));
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
