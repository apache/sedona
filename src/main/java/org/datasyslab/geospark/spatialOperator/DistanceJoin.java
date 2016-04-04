package org.datasyslab.geospark.spatialOperator;

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
import java.util.List;

import scala.Tuple2;

public class DistanceJoin {
    public static JavaPairRDD<Point, List<Point>> SpatialJoinQueryWithoutIndex(JavaSparkContext sc, PointRDD pointRDD1, PointRDD pointRDD2, Double distance) {
        //Grid filter, Maybe we can filter those key doesn't overlap the destination.

        //Just use grid of Convert pointRDD2 to CircleRDD.
        CircleRDD circleRDD2 = new CircleRDD(pointRDD2, distance);


        final Broadcast<ArrayList<EnvelopeWithGrid>> envelopeWithGrid = sc.broadcast(pointRDD1.grids);

        JavaPairRDD<Integer, Circle> tmpGridedCircleForQuerySetBeforePartition = circleRDD2.getCircleRDD().flatMapToPair(new PairFlatMapFunction<Circle, Integer, Circle>() {
            @Override
            public Iterable<Tuple2<Integer, Circle>> call(Circle circle) throws Exception {
                ArrayList<Tuple2<Integer, Circle>> result = new ArrayList<Tuple2<Integer, Circle>>();

                ArrayList<EnvelopeWithGrid> grid = envelopeWithGrid.getValue();

                for (EnvelopeWithGrid e : grid) {
                    try {
                        if (circle.intersects(e)) {
                            result.add(new Tuple2<Integer, Circle>(e.grid, circle));
                        }
                    } catch (NullPointerException exp) {
                        System.out.println(e.toString() + circle.toString());
                    }
                }
                return result;

            }
        });

        JavaPairRDD<Integer, Circle> tmpGridRDDForQuerySet = tmpGridedCircleForQuerySetBeforePartition.partitionBy(pointRDD1.gridPointRDD.partitioner().get()).persist(StorageLevel.DISK_ONLY());

        JavaPairRDD<Integer, Tuple2<Iterable<Point>, Iterable<Circle>>> cogroupResult = pointRDD1.gridPointRDD.cogroup(tmpGridRDDForQuerySet);

        JavaPairRDD<Point, HashSet<Point>> joinResultBeforeAggregation = cogroupResult.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<Point>, Iterable<Circle>>>, Point, HashSet<Point>>() {
            @Override
            public Iterable<Tuple2<Point, HashSet<Point>>> call(Tuple2<Integer, Tuple2<Iterable<Point>, Iterable<Circle>>> cogroup) throws Exception {
                ArrayList<Tuple2<Point, HashSet<Point>>> result = new ArrayList<Tuple2<Point, HashSet<Point>>>();

                Tuple2<Iterable<Point>, Iterable<Circle>> cogroupTupleList = cogroup._2();
                ArrayList<Point> points = new ArrayList<Point>();
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
                return result;
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


    public static JavaPairRDD<Point, List<Point>> SpatialJoinQueryUsingIndex(JavaSparkContext sc, PointRDD pointRDD1, PointRDD pointRDD2, Double distance, String indexType) {
        //Grid filter, Maybe we can filter those key doesn't overlap the destination.

        //Just use grid of Convert pointRDD2 to CircleRDD.
        CircleRDD circleRDD2 = new CircleRDD(pointRDD2, distance);

        //Build grid on circleRDD2.

        final Broadcast<ArrayList<EnvelopeWithGrid>> envelopeWithGrid = sc.broadcast(pointRDD1.grids);

        JavaPairRDD<Integer, Circle> tmpGridedCircleForQuerySetBeforePartition = circleRDD2.getCircleRDD().flatMapToPair(new PairFlatMapFunction<Circle, Integer, Circle>() {
            @Override
            public Iterable<Tuple2<Integer, Circle>> call(Circle circle) throws Exception {
                ArrayList<Tuple2<Integer, Circle>> result = new ArrayList<Tuple2<Integer, Circle>>();

                ArrayList<EnvelopeWithGrid> grid = envelopeWithGrid.getValue();

                for (EnvelopeWithGrid e : grid) {
                    if (circle.intersects(e)) {
                        result.add(new Tuple2<Integer, Circle>(e.grid, circle));
                    }
                }
                return result;

            }
        });

        JavaPairRDD<Integer, Circle> tmpGridRDDForQuerySet = tmpGridedCircleForQuerySetBeforePartition.partitionBy(pointRDD1.gridPointRDD.partitioner().get()).persist(StorageLevel.DISK_ONLY());

        JavaPairRDD<Integer, Tuple2<Iterable<STRtree>, Iterable<Circle>>> cogroupResult = pointRDD1.indexedRDD.cogroup(tmpGridRDDForQuerySet);

        JavaPairRDD<Point, HashSet<Point>> joinResultBeforeAggregation = cogroupResult.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<STRtree>, Iterable<Circle>>>, Point, HashSet<Point>>() {
            @Override
            public Iterable<Tuple2<Point, HashSet<Point>>> call(Tuple2<Integer, Tuple2<Iterable<STRtree>, Iterable<Circle>>> cogroup) throws Exception {
                ArrayList<Tuple2<Point, HashSet<Point>>> result = new ArrayList<Tuple2<Point, HashSet<Point>>>();

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
                return result;
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
