package org.datasyslab.geospark.spatialOperator;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.index.strtree.STRtree;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.geometryObjects.EnvelopeWithGrid;
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

        //Check if rawPointRDD have index.
        if(pointRDD.indexedRDD == null) {
            throw new NullPointerException("Need to invoke buildIndex() first, indexedRDD is null");
        }

        JavaPairRDD<Integer, Envelope> tmpGridRDDForQuerySet = getIntegerEnvelopeJavaPairRDD(sc, pointRDD, rectangleRDD);


        JavaPairRDD<Integer, Tuple2<Iterable<STRtree>, Iterable<Envelope>>> cogroupResult = pointRDD.indexedRDD.cogroup(tmpGridRDDForQuerySet);

        //flatMapToPair, use HashSet.
        //This will be really time consuiming.. But the memory usage will be less then previous solution.
        //todo: Verify this implementation reduce shuffle???
        JavaPairRDD<Envelope, HashSet<Point>> joinResultBeforeAggregation = cogroupResult.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<STRtree>, Iterable<Envelope>>>, Envelope, HashSet<Point>>() {
            @Override
            public Iterable<Tuple2<Envelope, HashSet<Point>>> call(Tuple2<Integer, Tuple2<Iterable<STRtree>, Iterable<Envelope>>> cogroup) throws Exception {
                ArrayList<Tuple2<Envelope, HashSet<Point>>> result = new ArrayList<Tuple2<Envelope, HashSet<Point>>>();

                Tuple2<Iterable<STRtree>, Iterable<Envelope>> cogroupTupleList = cogroup._2();
                for(Envelope e : cogroupTupleList._2()) {
                    List<Point> pointList = new ArrayList<Point>();
                    for(STRtree s:cogroupTupleList._1()) {
                        pointList = s.query(e);
                    }
                    HashSet<Point> pointSet = new HashSet<Point>(pointList);
                    result.add(new Tuple2<Envelope, HashSet<Point>>(e, pointSet));
                }
                return result;
            }

        });

        //AggregateByKey?
        JavaPairRDD<Envelope, List<Point>> joinListResultAfterAggregation = aggregateJoinResult(joinResultBeforeAggregation);

        return joinListResultAfterAggregation;
    }



    public static JavaPairRDD<Integer, Envelope> getIntegerEnvelopeJavaPairRDDAndCacheInMemory(JavaSparkContext sc, PointRDD pointRDD, RectangleRDD rectangleRDD) {
        //Build Grid, same as without Grid
        final Broadcast<ArrayList<EnvelopeWithGrid>> gridBroadcasted= sc.broadcast(pointRDD.grids);
        //todo: Add logic, if this is cached, no need to calculate it again.

        JavaPairRDD<Integer, Envelope> tmpGridRDDForQuerySetBeforePartition = rectangleRDD.getRawRectangleRDD().flatMapToPair(new PairFlatMapFunction<Envelope, Integer, Envelope>() {
            @Override
            public Iterable<Tuple2<Integer, Envelope>> call(Envelope envelope) throws Exception {
                ArrayList<Tuple2<Integer, Envelope>> result = new ArrayList<Tuple2<Integer, Envelope>>();

                ArrayList<EnvelopeWithGrid> grid = gridBroadcasted.getValue();

                for(EnvelopeWithGrid e:grid) {
                    if(e.intersects(envelope))
                        result.add(new Tuple2<Integer, Envelope>(e.grid, envelope));
                }
                return result;
            }
        });

        //Reparition, so that when cogroup, less data shuffle.
        //todo, change storage level to memory based on third parameter.
        return tmpGridRDDForQuerySetBeforePartition.partitionBy(pointRDD.gridPointRDD.partitioner().get()).cache();
    }

    public static JavaPairRDD<Envelope, List<Point>> SpatialJoinQueryWithOutIndex(JavaSparkContext sc, PointRDD pointRDD, RectangleRDD rectangleRDD, boolean cacheTmpGrid) {
        //todo: Add logic, if this is cached, no need to calculate it again.
        JavaPairRDD<Integer, Envelope> tmpGridRDDForQuerySet = getIntegerEnvelopeJavaPairRDD(sc, pointRDD, rectangleRDD);

        //cogroup
        JavaPairRDD<Integer, Tuple2<Iterable<Point>, Iterable<Envelope>>> cogroupResult = pointRDD.gridPointRDD.cogroup(tmpGridRDDForQuerySet);


        //flatMapToPair, use HashSet.
        //This will be really time consuiming.. But the memory usage will be less then in version 1.0
        //todo: Verify this implementation will reduce shuffle
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
        JavaPairRDD<Envelope, List<Point>> joinListResultAfterAggregation = aggregateJoinResult(joinResultBeforeAggregation);


        return joinListResultAfterAggregation;
        //Conver HashSet to a better way? May ArrayList,
    }

    public static JavaPairRDD<Envelope, List<Point>> aggregateJoinResult(JavaPairRDD<Envelope, HashSet<Point>> joinResultBeforeAggregation) {
        //AggregateByKey?
        JavaPairRDD<Envelope, HashSet<Point>> joinResultAfterAggregation = joinResultBeforeAggregation.reduceByKey(new Function2<HashSet<Point>, HashSet<Point>, HashSet<Point>>() {
            @Override
            public HashSet<Point> call(HashSet<Point> points, HashSet<Point> points2) throws Exception {
                points.addAll(points2);
                return points;
            }
        });


        return joinResultAfterAggregation.mapValues(new Function<HashSet<Point>, List<Point>>() {
            @Override
            public List<Point> call(HashSet<Point> points) throws Exception {
                return new ArrayList<Point>(points);
            }
        });
    }

    /*
        The method will take two parameters, one
        This method will create a grid RDD for rectangle,
     */
    public static JavaPairRDD<Integer, Envelope> getIntegerEnvelopeJavaPairRDD(JavaSparkContext sc, PointRDD pointRDD, RectangleRDD rectangleRDD) {
        //Build Grid, same as without Grid
        final Broadcast<ArrayList<EnvelopeWithGrid>> gridBroadcasted= sc.broadcast(pointRDD.grids);
        //todo: Add logic, if this is cached, no need to calculate it again.

        JavaPairRDD<Integer, Envelope> tmpGridRDDForQuerySetBeforePartition = rectangleRDD.getRawRectangleRDD().flatMapToPair(new PairFlatMapFunction<Envelope, Integer, Envelope>() {
            @Override
            public Iterable<Tuple2<Integer, Envelope>> call(Envelope envelope) throws Exception {
                ArrayList<Tuple2<Integer, Envelope>> result = new ArrayList<Tuple2<Integer, Envelope>>();

                ArrayList<EnvelopeWithGrid> grid = gridBroadcasted.getValue();

                for(EnvelopeWithGrid e:grid) {
                    if(e.intersects(envelope))
                        result.add(new Tuple2<Integer, Envelope>(e.grid, envelope));
                }
                return result;
            }
        });

        //Reparition, so that when cogroup, less data shuffle.
        //todo, change storage level to memory based on third parameter.
        return tmpGridRDDForQuerySetBeforePartition.partitionBy(pointRDD.gridPointRDD.partitioner().get()).persist(StorageLevel.DISK_ONLY());
    }

    //point join polygon




}
