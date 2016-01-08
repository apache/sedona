package org.datasyslab.geospark.spatialOperator;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.index.strtree.STRtree;

import org.apache.hadoop.util.hash.Hash;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import scala.Tuple2;

//todo: Replace older join query class.
public class NewJoinQuery implements Serializable{


    public static JavaPairRDD<Envelope, List<Point>> SpatialJoinQueryUsingIndex(JavaSparkContext sc, PointRDD pointRDD, RectangleRDD rectangleRDD, String indexType) {

        //Check if pointRDD have index.
        if(pointRDD.indexedRDD == null) {
            throw new NullPointerException("Need to invoke buildIndex() first, indexedRDD is null");
        }

        //Build Grid, same as without Grid
        final Broadcast<ArrayList<Double>> gridBroadcasted= sc.broadcast(pointRDD.grid);
        //todo: Add logic, if this is cached, no need to calculate it again.

        JavaPairRDD<Integer, Envelope> tmpGridRDDForQuerySetBeforePartition = rectangleRDD.getRectangleRDD().flatMapToPair(new PairFlatMapFunction<Envelope, Integer, Envelope>() {
            @Override
            public Iterable<Tuple2<Integer, Envelope>> call(Envelope envelope) throws Exception {
                ArrayList<Tuple2<Integer, Envelope>> result = new ArrayList<Tuple2<Integer, Envelope>>();

                ArrayList<Double> grid = gridBroadcasted.getValue();
                //todo: target1 is a bad name, change later.
                Double target1 = envelope.getMinX();
                //Find lower bound:
                int begin = 0, end = grid.size() - 1;

                int lowerbound = 0, upperbound = 0;
                while( begin < end ) {
                    int mid = (begin + end) / 2;
                    if(Double.compare(grid.get(mid).doubleValue(), target1) == -1) {
                        begin = mid + 1;
                    }
                    else
                        end = mid;
                }
                lowerbound = begin;

                begin = 0;
                end = grid.size() - 1;
                while(begin < end) {
                    int mid = (begin + end) / 2 + 1;
                    if(grid.get(mid).compareTo(envelope.getMaxX())> 0 ? true : false )
                        end = mid - 1;
                    else
                        begin = mid;
                }
                upperbound = end;

                for(int i = lowerbound; i <= upperbound; i++) {
                    result.add(new Tuple2<Integer, Envelope>(i, envelope));
                }
                return result;
            }
        });

        //Reparition, so that when cogroup, less data shuffle.
        //todo, change storage level to memory based on third parameter.
        JavaPairRDD<Integer, Envelope> tmpGridRDDForQuerySet = tmpGridRDDForQuerySetBeforePartition.partitionBy(pointRDD.gridPointRDD.partitioner().get()).persist(StorageLevel.DISK_ONLY());

        JavaPairRDD<Integer, Tuple2<Iterable<STRtree>, Iterable<Envelope>>> cogroupResult = pointRDD.indexedRDD.cogroup(tmpGridRDDForQuerySet);

        //flatMapToPair, use HashSet.
        //This will be really time consuiming.. But the memory usage will be less then previous solution.
        //todo: Will this implementation reduce shuffle???
        JavaPairRDD<Envelope, HashSet<Point>> joinResultBeforeAggregation = cogroupResult.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<STRtree>, Iterable<Envelope>>>, Envelope, HashSet<Point>>() {
            @Override
            public Iterable<Tuple2<Envelope, HashSet<Point>>> call(Tuple2<Integer, Tuple2<Iterable<STRtree>, Iterable<Envelope>>> cogroup) throws Exception {
                ArrayList<Tuple2<Envelope, HashSet<Point>>> result = new ArrayList<Tuple2<Envelope, HashSet<Point>>>();

                Tuple2<Iterable<STRtree>, Iterable<Envelope>> cogroupTupleList = cogroup._2();
                for(Envelope e : cogroupTupleList._2()) {
                    List<Point> pointList = new ArrayList<Point>();
                    for(STRtree s:cogroupTupleList._1()) {
                        //这可以? 他都不知道类型把..
                        pointList = s.query(e);
                    }
                    HashSet<Point> pointSet = new HashSet<Point>(pointList);
                    result.add(new Tuple2<Envelope, HashSet<Point>>(e, pointSet));
                }
                return result;
            }

        });

        //AggregateByKey?
        JavaPairRDD<Envelope, HashSet<Point>> joinResultAfterAggregation = joinResultBeforeAggregation.reduceByKey(new Function2<HashSet<Point>, HashSet<Point>, HashSet<Point>>() {
            @Override
            public HashSet<Point> call(HashSet<Point> points, HashSet<Point> points2) throws Exception {
                points.addAll(points2);
                return points;
            }
        });


        JavaPairRDD<Envelope, List<Point>> joinListResultAfterAggregation = joinResultAfterAggregation.mapValues(new Function<HashSet<Point>, List<Point>>() {
            @Override
            public List<Point> call(HashSet<Point> points) throws Exception {
                return new ArrayList<Point>(points);
            }
        });

        return joinListResultAfterAggregation;
    }

    public static JavaPairRDD<Envelope, List<Point>> SpatialJoinQuery(JavaSparkContext sc, PointRDD pointRDD, RectangleRDD rectangleRDD, boolean cacheTmpGrid) {
        //for rectangle, create a pairRDD use the sameGrid as pointRDD
        final Broadcast<ArrayList<Double>> gridBroadcasted= sc.broadcast(pointRDD.grid);
        //todo: Add logic, if this is cached, no need to calculate it again.

        JavaPairRDD<Integer, Envelope> tmpGridRDDForQuerySetBeforePartition = rectangleRDD.getRectangleRDD().flatMapToPair(new PairFlatMapFunction<Envelope, Integer, Envelope>() {
            @Override
            public Iterable<Tuple2<Integer, Envelope>> call(Envelope envelope) throws Exception {
                ArrayList<Tuple2<Integer, Envelope>> result = new ArrayList<Tuple2<Integer, Envelope>>();

                ArrayList<Double> grid = gridBroadcasted.getValue();
                //todo: target1 is a bad name, change later.
                Double target1 = envelope.getMinX();
                //Find lower bound:
                int begin = 0, end = grid.size() - 1;

                int lowerbound = 0, upperbound = 0;
                while( begin < end ) {
                    int mid = (begin + end) / 2;
                    if(Double.compare(grid.get(mid).doubleValue(), target1) == -1) {
                        begin = mid + 1;
                    }
                    else
                        end = mid;
                }
                lowerbound = begin;

                begin = 0;
                end = grid.size() - 1;
                while(begin < end) {
                    int mid = (begin + end) / 2 + 1;
                    if(grid.get(mid).compareTo(envelope.getMaxX())> 0 ? true : false )
                        end = mid - 1;
                    else
                        begin = mid;
                }
                upperbound = end;

                for(int i = lowerbound; i <= upperbound; i++) {
                    result.add(new Tuple2<Integer, Envelope>(i, envelope));
                }
                return result;
            }
        });

        //Reparition, so that when cogroup, less data shuffle.
        //todo, change storage level to memory based on third parameter.
        JavaPairRDD<Integer, Envelope> tmpGridRDDForQuerySet = tmpGridRDDForQuerySetBeforePartition.partitionBy(pointRDD.gridPointRDD.partitioner().get()).persist(StorageLevel.DISK_ONLY());


        //cogroup
        JavaPairRDD<Integer, Tuple2<Iterable<Point>, Iterable<Envelope>>> cogroupResult = pointRDD.gridPointRDD.cogroup(tmpGridRDDForQuerySet);


        //flatMapToPair, use HashSet.
        //This will be really time consuiming.. But the memory usage will be less then in version 1.0
        //todo: Will this implementation reduce shuffle???
        JavaPairRDD<Envelope, HashSet<Point>> joinResultBeforeAggregation = cogroupResult.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<Point>, Iterable<Envelope>>>, Envelope, HashSet<Point>>() {
            @Override
            public Iterable<Tuple2<Envelope, HashSet<Point>>> call(Tuple2<Integer, Tuple2<Iterable<Point>, Iterable<Envelope>>> cogroup) throws Exception {
                ArrayList<Tuple2<Envelope, HashSet<Point>>> result = new ArrayList<Tuple2<Envelope, HashSet<Point>>>();

                Tuple2<Iterable<Point>, Iterable<Envelope>> cogroupTupleList = cogroup._2();
                for (Envelope e : cogroupTupleList._2()) {
                    HashSet<Point> poinitHashSet = new HashSet<Point>();
                    for (Point p : cogroupTupleList._1()) {
                        if (e.contains(p.getCoordinate())) {
                            poinitHashSet.add(p);
                        }
                    }
                    result.add(new Tuple2<Envelope, HashSet<Point>>(e, poinitHashSet));
                }
                return result;
            }
        });


        //AggregateByKey?
        JavaPairRDD<Envelope, HashSet<Point>> joinResultAfterAggregation = joinResultBeforeAggregation.reduceByKey(new Function2<HashSet<Point>, HashSet<Point>, HashSet<Point>>() {
            @Override
            public HashSet<Point> call(HashSet<Point> points, HashSet<Point> points2) throws Exception {
                points.addAll(points2);
                return points;
            }
        });


        JavaPairRDD<Envelope, List<Point>> joinListResultAfterAggregation = joinResultAfterAggregation.mapValues(new Function<HashSet<Point>, List<Point>>() {
            @Override
            public List<Point> call(HashSet<Point> points) throws Exception {
                return new ArrayList<Point>(points);
            }
        });

        return joinListResultAfterAggregation;
        //Conver HashSet to a better way? May ArrayList,
    }
}
