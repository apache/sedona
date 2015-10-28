package org.datasyslab.geospark.spatialOperator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.datasyslab.geospark.boundryFilter.CircleFilterPoint;
import org.datasyslab.geospark.boundryFilter.PointPreFilter;
import org.datasyslab.geospark.boundryFilter.PolygonPreFilter;
import org.datasyslab.geospark.gemotryObjects.Circle;
import org.datasyslab.geospark.partition.PartitionAssignGridCircle;
import org.datasyslab.geospark.partition.PartitionAssignGridPoint;
import org.datasyslab.geospark.partition.PartitionAssignGridPolygon;
import org.datasyslab.geospark.partition.PartitionAssignGridRectangle;
import org.datasyslab.geospark.spatialRDD.CircleRDD;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;
import org.datasyslab.geospark.spatialRDD.SpatialPairRDD;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.quadtree.Quadtree;
import com.vividsolutions.jts.index.strtree.STRtree;

import scala.Tuple2;

public class JoinQuery {

	/**
	 * Spatial join query.
	 *
	 * @param rectangleRDD
	 *            the rectangle rdd
	 * @param Condition
	 *            the condition
	 * @param GridNumberHorizontal
	 *            the grid number horizontal
	 * @param GridNumberVertical
	 *            the grid number vertical
	 * @return the spatial pair rdd
	 */
	public static SpatialPairRDD<Envelope, ArrayList<Envelope>> SpatialJoinQuery(RectangleRDD TargetrectangleRDD,
			RectangleRDD queryWindowRDD, Integer Condition, Integer GridNumberHorizontal, Integer GridNumberVertical) {
		// Find the border of both of the two datasets---------------
		final Integer condition = Condition;
		// Find the border of both of the two datasets---------------
		// condition=0 means only consider fully contain in query, condition=1
		// means consider full contain and partial contain(overlap).
		// QueryAreaSet min/max longitude and latitude
		Envelope QueryWindowSetBoundary = queryWindowRDD.boundary();
		// TargetSet min/max longitude and latitude
		Envelope TargetSetBoundary = TargetrectangleRDD.boundary();
		Envelope boundary;
		// Border found
		JavaRDD<Envelope> TargetPreFiltered;
		JavaRDD<Envelope> QueryAreaPreFiltered;
		if (QueryWindowSetBoundary.contains(TargetSetBoundary)) {
			boundary = TargetSetBoundary;
			// TargetPreFiltered=this.rectangleRDD;
			// QueryAreaPreFiltered=rectangleRDD.rectangleRDD.filter(new
			// RectanglePreFilter(boundary));
		} else if (TargetSetBoundary.contains(QueryWindowSetBoundary)) {
			boundary = QueryWindowSetBoundary;
			// TargetPreFiltered=this.rectangleRDD.filter(new
			// RectanglePreFilter(boundary));
			// QueryAreaPreFiltered=rectangleRDD.getRectangleRDD();
		} else if (QueryWindowSetBoundary.intersects(TargetSetBoundary)) {
			boundary = QueryWindowSetBoundary.intersection(TargetSetBoundary);
			// TargetPreFiltered=this.rectangleRDD.filter(new
			// RectanglePreFilter(boundary));
			// QueryAreaPreFiltered=rectangleRDD.getRectangleRDD().filter(new
			// RectanglePreFilter(boundary));
		} else {
			System.out.println("Two input sets are not overlapped");
			return null;
		}
		// Build Grid file-------------------
		Double[] gridHorizontalBorder = new Double[GridNumberHorizontal + 1];
		Double[] gridVerticalBorder = new Double[GridNumberVertical + 1];
		double LongitudeIncrement = (boundary.getMaxX() - boundary.getMinX()) / GridNumberHorizontal;
		double LatitudeIncrement = (boundary.getMaxY() - boundary.getMinY()) / GridNumberVertical;
		for (int i = 0; i < GridNumberHorizontal + 1; i++) {
			gridHorizontalBorder[i] = boundary.getMinX() + LongitudeIncrement * i;
		}
		for (int i = 0; i < GridNumberVertical + 1; i++) {
			gridVerticalBorder[i] = boundary.getMinY() + LatitudeIncrement * i;
		}
		// Assign grid ID to both of the two dataset---------------------
		JavaPairRDD<Integer, Envelope> TargetSetWithIDtemp = TargetrectangleRDD.getRectangleRDD()
				.mapPartitionsToPair(new PartitionAssignGridRectangle(GridNumberHorizontal, GridNumberVertical,
						gridHorizontalBorder, gridVerticalBorder));
		JavaPairRDD<Integer, Envelope> QueryAreaSetWithIDtemp = queryWindowRDD.getRectangleRDD()
				.mapPartitionsToPair(new PartitionAssignGridRectangle(GridNumberHorizontal, GridNumberVertical,
						gridHorizontalBorder, gridVerticalBorder));
		// Remove cache from memory
		TargetrectangleRDD.getRectangleRDD().unpersist();
		queryWindowRDD.getRectangleRDD().unpersist();
		JavaPairRDD<Integer, Envelope> TargetSetWithID = TargetSetWithIDtemp;// .repartition(TargetSetWithIDtemp.partitions().size()*2);
		JavaPairRDD<Integer, Envelope> QueryAreaSetWithID = QueryAreaSetWithIDtemp;// .repartition(TargetSetWithIDtemp.partitions().size()*2);
		// Join two dataset
		JavaPairRDD<Integer, Tuple2<Envelope, Envelope>> jointSet1 = QueryAreaSetWithID.join(TargetSetWithID,
				TargetSetWithIDtemp.partitions().size() * 2);// .repartition((QueryAreaSetWithID.partitions().size()+TargetSetWithID.partitions().size())*2);
		// Calculate the relation between one point and one query area
		JavaPairRDD<Envelope, Envelope> jointSet = jointSet1
				.mapToPair(new PairFunction<Tuple2<Integer, Tuple2<Envelope, Envelope>>, Envelope, Envelope>() {

					public Tuple2<Envelope, Envelope> call(Tuple2<Integer, Tuple2<Envelope, Envelope>> t) {
						return t._2();
					}

				});
		JavaPairRDD<Envelope, Envelope> queryResult = jointSet
				.filter(new Function<Tuple2<Envelope, Envelope>, Boolean>() {

					public Boolean call(Tuple2<Envelope, Envelope> v1) {
						if (condition == 0) {
							if (v1._1().contains(v1._2())) {
								return true;
							} else
								return false;
						} else {
							if (v1._1().contains(v1._2()) || v1._1().intersects(v1._2())) {
								return true;
							} else
								return false;
						}
					}

				});
		// Delete the duplicate result
		JavaPairRDD<Envelope, Iterable<Envelope>> aggregatedResult = queryResult.groupByKey();
		JavaPairRDD<Envelope, ArrayList<Envelope>> refinedResult = aggregatedResult
				.mapToPair(new PairFunction<Tuple2<Envelope, Iterable<Envelope>>, Envelope, ArrayList<Envelope>>() {

					public Tuple2<Envelope, ArrayList<Envelope>> call(Tuple2<Envelope, Iterable<Envelope>> v) {
						ArrayList<Envelope> list = new ArrayList<Envelope>();
						ArrayList<Envelope> result = new ArrayList<Envelope>();
						Iterator<Envelope> targetIterator = v._2().iterator();
						while (targetIterator.hasNext()) {
							list.add(targetIterator.next());
						}

						for (int i = 0; i < list.size(); i++) {
							Integer duplicationFlag = 0;
							Envelope currentTargeti = list.get(i);
							for (int j = i + 1; j < list.size(); j++) {
								Envelope currentTargetj = list.get(j);
								if (currentTargeti.equals(currentTargetj)) {
									duplicationFlag = 1;
								}
							}
							if (duplicationFlag == 0) {
								result.add(currentTargeti);
							}
						}
						return new Tuple2<Envelope, ArrayList<Envelope>>(v._1(), result);
					}

				});
		SpatialPairRDD<Envelope, ArrayList<Envelope>> result = new SpatialPairRDD<Envelope, ArrayList<Envelope>>(
				refinedResult);
		return result;
	}

	/**
	 * Spatial join query.
	 *
	 * @param queryWindowpolygonRDD
	 *            the polygon rdd
	 * @param Condition
	 *            the condition
	 * @param GridNumberHorizontal
	 *            the grid number horizontal
	 * @param GridNumberVertical
	 *            the grid number vertical
	 * @return the spatial pair rdd
	 */
	public static SpatialPairRDD<Polygon, ArrayList<Polygon>> SpatialJoinQuery(PolygonRDD targetPolygonRDD,
			PolygonRDD queryWindowpolygonRDD, Integer Condition, Integer GridNumberHorizontal,
			Integer GridNumberVertical) {
		// Find the border of both of the two datasets---------------
		final Integer condition = Condition;
		// Find the border of both of the two datasets---------------
		// condition=0 means only consider fully contain in query, condition=1
		// means consider full contain and partial contain(overlap).
		// QueryAreaSet min/max longitude and latitude
		Envelope QueryWindowSetBoundary = queryWindowpolygonRDD.boundary();
		// TargetSet min/max longitude and latitude
		Envelope TargetSetBoundary = targetPolygonRDD.boundary();
		Envelope boundary;
		// Border found
		JavaRDD<Polygon> TargetPreFiltered;
		JavaRDD<Polygon> QueryAreaPreFiltered;
		if (QueryWindowSetBoundary.contains(TargetSetBoundary)) {
			boundary = TargetSetBoundary;
			TargetPreFiltered = targetPolygonRDD.getPolygonRDD();
			QueryAreaPreFiltered = queryWindowpolygonRDD.getPolygonRDD().filter(new PolygonPreFilter(boundary));
		} else if (TargetSetBoundary.contains(QueryWindowSetBoundary)) {
			boundary = QueryWindowSetBoundary;
			TargetPreFiltered = targetPolygonRDD.getPolygonRDD().filter(new PolygonPreFilter(boundary));
			QueryAreaPreFiltered = queryWindowpolygonRDD.getPolygonRDD();
		} else if (QueryWindowSetBoundary.intersects(TargetSetBoundary)) {
			boundary = QueryWindowSetBoundary.intersection(TargetSetBoundary);
			TargetPreFiltered = targetPolygonRDD.getPolygonRDD().filter(new PolygonPreFilter(boundary));
			QueryAreaPreFiltered = queryWindowpolygonRDD.getPolygonRDD().filter(new PolygonPreFilter(boundary));
		} else {
			System.out.println("Two input sets are not overlapped");
			return null;
		}
		// Build Grid file-------------------
		Double[] gridHorizontalBorder = new Double[GridNumberHorizontal + 1];
		Double[] gridVerticalBorder = new Double[GridNumberVertical + 1];
		double LongitudeIncrement = (boundary.getMaxX() - boundary.getMinX()) / GridNumberHorizontal;
		double LatitudeIncrement = (boundary.getMaxY() - boundary.getMinY()) / GridNumberVertical;
		for (int i = 0; i < GridNumberHorizontal + 1; i++) {
			gridHorizontalBorder[i] = boundary.getMinX() + LongitudeIncrement * i;
		}
		for (int i = 0; i < GridNumberVertical + 1; i++) {
			gridVerticalBorder[i] = boundary.getMinY() + LatitudeIncrement * i;
		}

		// Assign grid ID to both of the two dataset---------------------
		JavaPairRDD<Integer, Polygon> TargetSetWithID = targetPolygonRDD.getPolygonRDD()
				.mapPartitionsToPair(new PartitionAssignGridPolygon(GridNumberHorizontal, GridNumberVertical,
						gridHorizontalBorder, gridVerticalBorder));
		JavaPairRDD<Integer, Polygon> QueryAreaSetWithID = queryWindowpolygonRDD.getPolygonRDD()
				.mapPartitionsToPair(new PartitionAssignGridPolygon(GridNumberHorizontal, GridNumberVertical,
						gridHorizontalBorder, gridVerticalBorder));
		// Remove cache from memory
		targetPolygonRDD.getPolygonRDD().unpersist();
		queryWindowpolygonRDD.getPolygonRDD().unpersist();
		// Join two dataset
		JavaPairRDD<Integer, Tuple2<Polygon, Polygon>> jointSet1 = QueryAreaSetWithID.join(TargetSetWithID,
				TargetSetWithID.partitions().size() * 2);// .repartition((QueryAreaSetWithID.partitions().size()+TargetSetWithID.partitions().size())*2);
		// Calculate the relation between one point and one query area
		JavaPairRDD<Polygon, Polygon> jointSet = jointSet1
				.mapToPair(new PairFunction<Tuple2<Integer, Tuple2<Polygon, Polygon>>, Polygon, Polygon>() {

					public Tuple2<Polygon, Polygon> call(Tuple2<Integer, Tuple2<Polygon, Polygon>> t) {
						return t._2();
					}

				});
		JavaPairRDD<Polygon, Polygon> queryResult = jointSet.filter(new Function<Tuple2<Polygon, Polygon>, Boolean>() {

			public Boolean call(Tuple2<Polygon, Polygon> v1) {
				if (condition == 0) {
					if (v1._1().contains(v1._2())) {
						return true;
					} else
						return false;
				} else {
					if (v1._1().contains(v1._2()) || v1._1().intersects(v1._2())) {
						return true;
					} else
						return false;
				}
			}

		});
		// Delete the duplicate result
		JavaPairRDD<Polygon, Iterable<Polygon>> aggregatedResult = queryResult.groupByKey();
		JavaPairRDD<Polygon, String> refinedResult = aggregatedResult
				.mapToPair(new PairFunction<Tuple2<Polygon, Iterable<Polygon>>, Polygon, String>() {

					public Tuple2<Polygon, String> call(Tuple2<Polygon, Iterable<Polygon>> t) {
						Integer commaFlag = 0;
						Iterator<Polygon> valueIterator = t._2().iterator();
						String result = "";
						while (valueIterator.hasNext()) {
							Polygon currentTarget = valueIterator.next();
							Coordinate[] polygonCoordinate = currentTarget.getCoordinates();
							Integer count = polygonCoordinate.length;
							String currentTargetString = "";
							for (int i = 0; i < count; i++) {
								if (currentTargetString == "") {
									currentTargetString = currentTargetString + polygonCoordinate[i].x + ","
											+ polygonCoordinate[i].y;

								} else
									currentTargetString = currentTargetString + "," + polygonCoordinate[i].x + ","
											+ polygonCoordinate[i].y;
							}
							if (!result.contains(currentTargetString)) {
								if (commaFlag == 0) {
									result = result + currentTargetString;
									commaFlag = 1;
								} else
									result = result + ";" + currentTargetString;
							}
						}

						return new Tuple2<Polygon, String>(t._1(), result);
					}

				});
		// Return the result
		SpatialPairRDD<Polygon, ArrayList<Polygon>> result = new SpatialPairRDD<Polygon, ArrayList<Polygon>>(
				refinedResult.mapToPair(new PairFunction<Tuple2<Polygon, String>, Polygon, ArrayList<Polygon>>() {

					public Tuple2<Polygon, ArrayList<Polygon>> call(Tuple2<Polygon, String> t) {
						List<String> input = Arrays.asList(t._2().split(";"));
						Iterator<String> inputIterator = input.iterator();
						ArrayList<Polygon> resultList = new ArrayList<Polygon>();
						while (inputIterator.hasNext()) {
							List<String> resultListString = Arrays.asList(inputIterator.next().split(","));
							Iterator<String> targetIterator = resultListString.iterator();
							ArrayList<Coordinate> coordinatesList = new ArrayList<Coordinate>();
							while (targetIterator.hasNext()) {
								coordinatesList.add(new Coordinate(Double.parseDouble(targetIterator.next()),
										Double.parseDouble(targetIterator.next())));
							}
							Coordinate[] coordinates = new Coordinate[coordinatesList.size()];
							coordinates = coordinatesList.toArray(coordinates);
							GeometryFactory fact = new GeometryFactory();
							LinearRing linear = new GeometryFactory().createLinearRing(coordinates);
							Polygon polygon = new Polygon(linear, null, fact);
							resultList.add(polygon);
						}
						return new Tuple2<Polygon, ArrayList<Polygon>>(t._1(), resultList);
					}

				}));
		return result;

	}

	/**
	 * Spatial join query.
	 *
	 * @param circleRDD
	 *                        the circle rdd
	 * @param Radius
	 *                        the radius
	 * @param Condition
	 *                        the condition
	 * @param GridNumberHorizontal
	 *                        the grid number horizontal
	 * @param GridNumberVertical
	 *                        the grid number vertical
	 * @return the spatial pair rdd
	 */
	public static SpatialPairRDD<Point, ArrayList<Point>> SpatialJoinQuery(PointRDD pointRDD, CircleRDD circleRDD,
			Double Radius, Integer Condition, Integer GridNumberHorizontal, Integer GridNumberVertical) {
		// Find the border of both of the two datasets---------------
		// condition=0 means only consider fully contain in query, condition=1
		// means consider full contain and partial contain(overlap).
		// QueryAreaSet min/max longitude and latitude
		Envelope QueryWindowSetBoundary = circleRDD.boundary();
		// TargetSet min/max longitude and latitude
		Envelope TargetSetBoundary = pointRDD.boundary();
		Envelope boundary;
		// Border found
		JavaRDD<Point> TargetPreFiltered;
		JavaRDD<Circle> QueryAreaPreFiltered;
		// Integer
		// currentPartitionsTargetSet=this.pointRDD.partitions().size()/2;
		// Integer
		// currentPartitionQuerySet=circleRDD.getCircleRDD().partitions().size();
		if (QueryWindowSetBoundary.contains(TargetSetBoundary)) {
			boundary = TargetSetBoundary;
			// TargetPreFiltered=this.pointRDD;
			// QueryAreaPreFiltered=circleRDD.getCircleRDD().filter(new
			// CirclePreFilter(boundary));
		} else if (TargetSetBoundary.contains(QueryWindowSetBoundary)) {
			boundary = QueryWindowSetBoundary;
			// TargetPreFiltered=this.pointRDD.filter(new
			// PointPreFilter(boundary));//.repartition(currentPartitionsTargetSet);
			// QueryAreaPreFiltered=circleRDD.getCircleRDD();
		} else if (QueryWindowSetBoundary.intersects(TargetSetBoundary)) {
			boundary = QueryWindowSetBoundary.intersection(TargetSetBoundary);
			// TargetPreFiltered=this.pointRDD.filter(new
			// PointPreFilter(boundary));//.repartition(currentPartitionsTargetSet);
			// QueryAreaPreFiltered=circleRDD.getCircleRDD().filter(new
			// CirclePreFilter(boundary));
		} else {
			System.out.println("Two input sets are not overlapped");
			return null;
		}

		// Build Grid file-------------------
		Double[] gridHorizontalBorder = new Double[GridNumberHorizontal + 1];
		Double[] gridVerticalBorder = new Double[GridNumberVertical + 1];
		double LongitudeIncrement = (boundary.getMaxX() - boundary.getMinX()) / GridNumberHorizontal;
		double LatitudeIncrement = (boundary.getMaxY() - boundary.getMinY()) / GridNumberVertical;
		for (int i = 0; i < GridNumberHorizontal + 1; i++) {
			gridHorizontalBorder[i] = boundary.getMinX() + LongitudeIncrement * i;
		}
		for (int i = 0; i < GridNumberVertical + 1; i++) {
			gridVerticalBorder[i] = boundary.getMinY() + LatitudeIncrement * i;
		}
		// Assign grid ID to both of the two dataset---------------------

		JavaPairRDD<Integer, Point> TargetSetWithIDtemp = pointRDD.getPointRDD()
				.mapPartitionsToPair(new PartitionAssignGridPoint(GridNumberHorizontal, GridNumberVertical,
						gridHorizontalBorder, gridVerticalBorder));
		JavaPairRDD<Integer, Circle> QueryAreaSetWithID1 = circleRDD.getCircleRDD()
				.mapPartitionsToPair(new PartitionAssignGridCircle(GridNumberHorizontal, GridNumberVertical,
						gridHorizontalBorder, gridVerticalBorder));
		pointRDD.getPointRDD().unpersist();
		circleRDD.getCircleRDD().unpersist();
		JavaPairRDD<Integer, Point> QueryAreaSetWithIDtemp = QueryAreaSetWithID1
				.mapToPair(new PairFunction<Tuple2<Integer, Circle>, Integer, Point>() {

					public Tuple2<Integer, Point> call(Tuple2<Integer, Circle> t) {

						return new Tuple2<Integer, Point>(t._1(), t._2().getCentre());
					}

				});
		JavaPairRDD<Integer, Point> TargetSetWithID = TargetSetWithIDtemp;// .repartition(TargetSetWithIDtemp.partitions().size()*2);
		JavaPairRDD<Integer, Point> QueryAreaSetWithID = QueryAreaSetWithIDtemp;// .repartition(TargetSetWithIDtemp.partitions().size()*2);
		// Join two dataset
		JavaPairRDD<Integer, Tuple2<Point, Point>> jointSet = QueryAreaSetWithID.join(TargetSetWithID,
				TargetSetWithIDtemp.partitions().size() * 2);// .repartition((QueryAreaSetWithID.partitions().size()+TargetSetWithID.partitions().size())*2);
		// Calculate the relation between one point and one query area
		JavaPairRDD<Point, Point> jointSet1 = jointSet
				.mapToPair(new PairFunction<Tuple2<Integer, Tuple2<Point, Point>>, Point, Point>() {

					public Tuple2<Point, Point> call(Tuple2<Integer, Tuple2<Point, Point>> t) {
						return t._2();

					}

				});
		JavaPairRDD<Point, Point> queryResult = jointSet1.filter(new CircleFilterPoint(Radius, Condition));
		// Delete the duplicate result
		JavaPairRDD<Point, Iterable<Point>> aggregatedResult = queryResult.groupByKey();
		JavaPairRDD<Point, ArrayList<Point>> refinedResult = aggregatedResult
				.mapToPair(new PairFunction<Tuple2<Point, Iterable<Point>>, Point, ArrayList<Point>>() {

					public Tuple2<Point, ArrayList<Point>> call(Tuple2<Point, Iterable<Point>> v) {
						ArrayList<Point> list = new ArrayList<Point>();
						ArrayList<Point> result = new ArrayList<Point>();
						Iterator<Point> targetIterator = v._2().iterator();
						while (targetIterator.hasNext()) {
							list.add(targetIterator.next());
						}

						for (int i = 0; i < list.size(); i++) {
							Integer duplicationFlag = 0;
							Point currentPointi = list.get(i);
							for (int j = i + 1; j < list.size(); j++) {
								Point currentPointj = list.get(j);
								if (currentPointi.equals(currentPointj)) {
									duplicationFlag = 1;
								}
							}
							if (duplicationFlag == 0) {
								result.add(currentPointi);
							}
						}
						return new Tuple2<Point, ArrayList<Point>>(v._1(), result);
					}

				});
		SpatialPairRDD<Point, ArrayList<Point>> result = new SpatialPairRDD<Point, ArrayList<Point>>(refinedResult);
		return result;
	}

	/**
	 * Spatial join query.
	 *
	 * @param polygonRDD
	 *                        the polygon rdd
	 * @param Condition
	 *                        the condition
	 * @param GridNumberHorizontal
	 *                        the grid number horizontal
	 * @param GridNumberVertical
	 *                        the grid number vertical
	 * @return the spatial pair rdd
	 */
	public static SpatialPairRDD<Polygon, ArrayList<Point>> SpatialJoinQuery(PointRDD pointRDD, PolygonRDD polygonRDD,
			Integer Condition, Integer GridNumberHorizontal, Integer GridNumberVertical) {
		// Find the border of both of the two datasets---------------
		final Integer condition = Condition;
		// condition=0 means only consider fully contain in query, condition=1
		// means consider full contain and partial contain(overlap).
		// QueryAreaSet min/max longitude and latitude
		Envelope QueryWindowSetBoundary = polygonRDD.boundary();
		// TargetSet min/max longitude and latitude
		Envelope TargetSetBoundary = pointRDD.boundary();
		Envelope boundary = QueryWindowSetBoundary;

		// Border found

		if (QueryWindowSetBoundary.contains(TargetSetBoundary)) {
			boundary = TargetSetBoundary;
		} else if (TargetSetBoundary.contains(QueryWindowSetBoundary)) {
			boundary = QueryWindowSetBoundary;
		} else if (QueryWindowSetBoundary.intersects(TargetSetBoundary)) {
			boundary = QueryWindowSetBoundary.intersection(TargetSetBoundary);
		} else {
			System.out.println("Two input sets are not overlapped");
			return null;
		}
		JavaRDD<Point> TargetPreFiltered = pointRDD.getPointRDD().filter(new PointPreFilter(boundary));
		JavaRDD<Polygon> QueryAreaPreFiltered = polygonRDD.getPolygonRDD().filter(new PolygonPreFilter(boundary));
		// Build Grid file-------------------
		Double[] gridHorizontalBorder = new Double[GridNumberHorizontal + 1];
		Double[] gridVerticalBorder = new Double[GridNumberVertical + 1];
		double LongitudeIncrement = (boundary.getMaxX() - boundary.getMinX()) / GridNumberHorizontal;
		double LatitudeIncrement = (boundary.getMaxY() - boundary.getMinY()) / GridNumberVertical;
		for (int i = 0; i < GridNumberHorizontal + 1; i++) {
			gridHorizontalBorder[i] = boundary.getMinX() + LongitudeIncrement * i;
		}
		for (int i = 0; i < GridNumberVertical + 1; i++) {
			gridVerticalBorder[i] = boundary.getMinY() + LatitudeIncrement * i;
		}
		// Assign grid ID to both of the two dataset---------------------
		JavaPairRDD<Integer, Point> TargetSetWithIDtemp = TargetPreFiltered
				.mapPartitionsToPair(new PartitionAssignGridPoint(GridNumberHorizontal, GridNumberVertical,
						gridHorizontalBorder, gridVerticalBorder));
		JavaPairRDD<Integer, Polygon> QueryAreaSetWithIDtemp = QueryAreaPreFiltered
				.mapPartitionsToPair(new PartitionAssignGridPolygon(GridNumberHorizontal, GridNumberVertical,
						gridHorizontalBorder, gridVerticalBorder));
		// Remove cache from memory
		pointRDD.getPointRDD().unpersist();
		polygonRDD.getPolygonRDD().unpersist();
		JavaPairRDD<Integer, Point> TargetSetWithID = TargetSetWithIDtemp;// .repartition(TargetSetWithIDtemp.partitions().size()*2);
		JavaPairRDD<Integer, Polygon> QueryAreaSetWithID = QueryAreaSetWithIDtemp;// .repartition(TargetSetWithIDtemp.partitions().size()*2);
		// Join two dataset
		JavaPairRDD<Integer, Tuple2<Polygon, Point>> joinSet1 = QueryAreaSetWithID.join(TargetSetWithID,
				TargetSetWithIDtemp.partitions().size() * 2);
		JavaPairRDD<Polygon, Point> joinSet = joinSet1
				.mapToPair(new PairFunction<Tuple2<Integer, Tuple2<Polygon, Point>>, Polygon, Point>() {

					public Tuple2<Polygon, Point> call(Tuple2<Integer, Tuple2<Polygon, Point>> t) throws Exception {
						return t._2();
					}

				});
		// Delete the duplicate result
		JavaPairRDD<Polygon, Iterable<Point>> aggregatedResult = joinSet.groupByKey();
		JavaPairRDD<Polygon, String> refinedResult = aggregatedResult
				.mapToPair(new PairFunction<Tuple2<Polygon, Iterable<Point>>, Polygon, String>() {

					public Tuple2<Polygon, String> call(Tuple2<Polygon, Iterable<Point>> t) {
						Integer commaFlag = 0;
						Iterator<Point> valueIterator = t._2().iterator();
						String result = "";
						while (valueIterator.hasNext()) {
							Point currentTarget = valueIterator.next();
							String currentTargetString = "" + currentTarget.getX() + "," + currentTarget.getY();
							if (!result.contains(currentTargetString)) {
								if (commaFlag == 0) {
									result = result + currentTargetString;
									commaFlag = 1;
								} else
									result = result + "," + currentTargetString;
							}
						}

						return new Tuple2<Polygon, String>(t._1(), result);
					}

				});
		SpatialPairRDD<Polygon, ArrayList<Point>> result = new SpatialPairRDD<Polygon, ArrayList<Point>>(
				refinedResult.mapToPair(new PairFunction<Tuple2<Polygon, String>, Polygon, ArrayList<Point>>() {

					public Tuple2<Polygon, ArrayList<Point>> call(Tuple2<Polygon, String> t) {
						List<String> resultListString = Arrays.asList(t._2().split(","));
						Iterator<String> targetIterator = resultListString.iterator();
						ArrayList<Point> resultList = new ArrayList<Point>();
						while (targetIterator.hasNext()) {
							GeometryFactory fact = new GeometryFactory();
							Coordinate coordinate = new Coordinate(Double.parseDouble(targetIterator.next()),
									Double.parseDouble(targetIterator.next()));
							Point currentTarget = fact.createPoint(coordinate);
							resultList.add(currentTarget);
						}
						return new Tuple2<Polygon, ArrayList<Point>>(t._1(), resultList);
					}

				}));
		return result;
	}

	/**
	 * Spatial join query.
	 *
	 * @param rectangleRDD
	 *                        the rectangle rdd
	 * @param Condition
	 *                        the condition
	 * @param GridNumberHorizontal
	 *                        the grid number horizontal
	 * @param GridNumberVertical
	 *                        the grid number vertical
	 * @return the spatial pair rdd
	 */
	public static SpatialPairRDD<Envelope, ArrayList<Point>> SpatialJoinQuery(PointRDD pointRDD,
			RectangleRDD rectangleRDD, Integer Condition, Integer GridNumberHorizontal, Integer GridNumberVertical) {
		// Find the border of both of the two datasets---------------
		final Integer condition = Condition;
		// condition=0 means only consider fully contain in query, condition=1
		// means consider full contain and partial contain(overlap).
		// QueryAreaSet min/max longitude and latitude
		Envelope QueryWindowSetBoundary = rectangleRDD.boundary();
		// TargetSet min/max longitude and latitude
		Envelope TargetSetBoundary = pointRDD.boundary();
		Envelope boundary = QueryWindowSetBoundary;
		// Integer
		// currentPartitionsTargetSet=this.pointRDD.partitions().size()/2;
		// Integer
		// currentPartitionQuerySet=rectangleRDD.getRectangleRDD().partitions().size();
		// Border found
		JavaRDD<Point> TargetPreFiltered;// =this.pointRDD.filter(new
		// PointPreFilter(boundary));
		JavaRDD<Envelope> QueryAreaPreFiltered;// =rectangleRDD.getRectangleRDD().filter(new
		// RectanglePreFilter(boundary));
		if (QueryWindowSetBoundary.contains(TargetSetBoundary)) {
			boundary = TargetSetBoundary;
			// TargetPreFiltered=this.pointRDD;
			// QueryAreaPreFiltered=rectangleRDD.getRectangleRDD().filter(new
			// RectanglePreFilter(boundary));
		} else if (TargetSetBoundary.contains(QueryWindowSetBoundary)) {
			boundary = QueryWindowSetBoundary;
			// TargetPreFiltered=this.pointRDD.filter(new
			// PointPreFilter(boundary));//.repartition(currentPartitionsTargetSet);
			// QueryAreaPreFiltered=rectangleRDD.getRectangleRDD();
		} else if (QueryWindowSetBoundary.intersects(TargetSetBoundary)) {
			boundary = QueryWindowSetBoundary.intersection(TargetSetBoundary);
			// TargetPreFiltered=this.pointRDD.filter(new
			// PointPreFilter(boundary));//.repartition(currentPartitionsTargetSet);
			// QueryAreaPreFiltered=rectangleRDD.getRectangleRDD().filter(new
			// RectanglePreFilter(boundary));
		} else {
			System.out.println("Two input sets are not overlapped");
			return null;
		}
		// JavaRDD<Point> TargetPreFiltered=this.pointRDD.filter(new
		// PointPreFilter(boundary));
		// JavaRDD<Envelope>
		// QueryAreaPreFiltered=rectangleRDD.getRectangleRDD().filter(new
		// RectanglePreFilter(boundary));
		// Build Grid file-------------------
		Double[] gridHorizontalBorder = new Double[GridNumberHorizontal + 1];
		Double[] gridVerticalBorder = new Double[GridNumberVertical + 1];
		double LongitudeIncrement = (boundary.getMaxX() - boundary.getMinX()) / GridNumberHorizontal;
		double LatitudeIncrement = (boundary.getMaxY() - boundary.getMinY()) / GridNumberVertical;
		for (int i = 0; i < GridNumberHorizontal + 1; i++) {
			gridHorizontalBorder[i] = boundary.getMinX() + LongitudeIncrement * i;
		}
		for (int i = 0; i < GridNumberVertical + 1; i++) {
			gridVerticalBorder[i] = boundary.getMinY() + LatitudeIncrement * i;
		}
		// Assign grid ID to both of the two dataset---------------------
		JavaPairRDD<Integer, Point> TargetSetWithIDtemp = pointRDD.getPointRDD()
				.mapPartitionsToPair(new PartitionAssignGridPoint(GridNumberHorizontal, GridNumberVertical,
						gridHorizontalBorder, gridVerticalBorder));
		JavaPairRDD<Integer, Envelope> QueryAreaSetWithIDtemp = rectangleRDD.getRectangleRDD()
				.mapPartitionsToPair(new PartitionAssignGridRectangle(GridNumberHorizontal, GridNumberVertical,
						gridHorizontalBorder, gridVerticalBorder));
		// Remove cache from memory
		pointRDD.getPointRDD().unpersist();
		rectangleRDD.getRectangleRDD().unpersist();
		JavaPairRDD<Integer, Point> TargetSetWithID = TargetSetWithIDtemp;// .repartition(TargetSetWithIDtemp.partitions().size()*2);
		JavaPairRDD<Integer, Envelope> QueryAreaSetWithID = QueryAreaSetWithIDtemp;// .repartition(TargetSetWithIDtemp.partitions().size()*2);
		// Join two dataset
		JavaPairRDD<Integer, Tuple2<Envelope, Point>> joinSet1 = QueryAreaSetWithID.join(TargetSetWithID,
				TargetSetWithIDtemp.partitions().size() * 2);
		JavaPairRDD<Envelope, Point> joinSet = joinSet1
				.mapToPair(new PairFunction<Tuple2<Integer, Tuple2<Envelope, Point>>, Envelope, Point>() {

					public Tuple2<Envelope, Point> call(Tuple2<Integer, Tuple2<Envelope, Point>> t) throws Exception {
						return t._2();
					}

				});
		JavaPairRDD<Envelope, Point> queryResult = joinSet.filter(new Function<Tuple2<Envelope, Point>, Boolean>() {

			public Boolean call(Tuple2<Envelope, Point> v1) throws Exception {
				// TODO Auto-generated method stub
				if (condition == 0) {
					if (v1._1().intersects(v1._2().getCoordinate())) {
						return true;
					} else
						return false;
				} else {
					if (v1._1().contains(v1._2().getCoordinate())) {
						return true;
					} else
						return false;
				}
			}
		});
		// Delete the duplicate result
		JavaPairRDD<Envelope, Iterable<Point>> aggregatedResult = queryResult.groupByKey();
		JavaPairRDD<Envelope, ArrayList<Point>> refinedResult = aggregatedResult
				.mapToPair(new PairFunction<Tuple2<Envelope, Iterable<Point>>, Envelope, ArrayList<Point>>() {

					public Tuple2<Envelope, ArrayList<Point>> call(Tuple2<Envelope, Iterable<Point>> v) {
						ArrayList<Point> list = new ArrayList<Point>();
						ArrayList<Point> result = new ArrayList<Point>();
						Iterator<Point> targetIterator = v._2().iterator();
						while (targetIterator.hasNext()) {
							list.add(targetIterator.next());
						}

						for (int i = 0; i < list.size(); i++) {
							Integer duplicationFlag = 0;
							Point currentPointi = list.get(i);
							for (int j = i + 1; j < list.size(); j++) {
								Point currentPointj = list.get(j);
								if (currentPointi.equals(currentPointj)) {
									duplicationFlag = 1;
								}
							}
							if (duplicationFlag == 0) {
								result.add(currentPointi);
							}
						}
						return new Tuple2<Envelope, ArrayList<Point>>(v._1(), result);
					}

				});
		SpatialPairRDD<Envelope, ArrayList<Point>> result = new SpatialPairRDD<Envelope, ArrayList<Point>>(
				refinedResult);
		return result;

	}
	
	public SpatialPairRDD<Envelope,ArrayList<Envelope>> SpatialSelfJoinQueryWithIndex(RectangleRDD rectangleRDD, Integer Condition,Integer GridNumberHorizontal,Integer GridNumberVertical,String Index)
	{
		//Find the border of both of the two datasets---------------
		final String index=Index;
		final Integer condition=Condition;
		//Find the border of both of the two datasets---------------
				//condition=0 means only consider fully contain in query, condition=1 means consider full contain and partial contain(overlap).
				//QueryAreaSet min/max longitude and latitude
				//TargetSet min/max longitude and latitude
				Envelope boundary=rectangleRDD.boundary();;
				//Border found
	
		//Build Grid file-------------------
				Double[] gridHorizontalBorder = new Double[GridNumberHorizontal+1];
				Double[] gridVerticalBorder=new Double[GridNumberVertical+1];
				double LongitudeIncrement=(boundary.getMaxX()-boundary.getMinX())/GridNumberHorizontal;
				double LatitudeIncrement=(boundary.getMaxY()-boundary.getMinY())/GridNumberVertical;
				for(int i=0;i<GridNumberHorizontal+1;i++)
				{
					gridHorizontalBorder[i]=boundary.getMinX()+LongitudeIncrement*i;
				}
				for(int i=0;i<GridNumberVertical+1;i++)
				{
					gridVerticalBorder[i]=boundary.getMinY()+LatitudeIncrement*i;
				}
		//Assign grid ID to both of the two dataset---------------------
		JavaPairRDD<Integer,Envelope> TargetSetWithIDtemp=rectangleRDD.getRectangleRDD().mapPartitionsToPair(new PartitionAssignGridRectangle(GridNumberHorizontal,GridNumberVertical,gridHorizontalBorder,gridVerticalBorder));
		JavaPairRDD<Integer,Envelope> QueryAreaSetWithIDtemp=TargetSetWithIDtemp;
		//Remove cache from memory
		rectangleRDD.getRectangleRDD().unpersist();
		JavaPairRDD<Integer,Envelope> TargetSetWithID=TargetSetWithIDtemp;//.repartition(TargetSetWithIDtemp.partitions().size()*2);
		JavaPairRDD<Integer,Envelope> QueryAreaSetWithID=QueryAreaSetWithIDtemp;//.repartition(TargetSetWithIDtemp.partitions().size()*2);
//Join two dataset
		JavaPairRDD<Integer,Tuple2<Iterable<Envelope>,Iterable<Envelope>>> cogroupSet=QueryAreaSetWithID.cogroup(TargetSetWithID, TargetSetWithIDtemp.partitions().size()*2);
		
		JavaPairRDD<Envelope,ArrayList<Envelope>> queryResult=cogroupSet.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer,Tuple2<Iterable<Envelope>,Iterable<Envelope>>>,Envelope,ArrayList<Envelope>>()
				{

					public Iterable<Tuple2<Envelope, ArrayList<Envelope>>> call(
							Tuple2<Integer, Tuple2<Iterable<Envelope>, Iterable<Envelope>>> t)
					{
						
							if(index=="quadtree")
							{
								Quadtree qt=new Quadtree();
								Iterator<Envelope> targetIterator=t._2()._2().iterator();
								Iterator<Envelope> queryAreaIterator=t._2()._1().iterator();
								ArrayList<Tuple2<Envelope,ArrayList<Envelope>>> result=new ArrayList();
								while(targetIterator.hasNext())
								{
									Envelope currentTarget=targetIterator.next();
									qt.insert(currentTarget, currentTarget);
								}
								while(queryAreaIterator.hasNext())
								{
									Envelope currentQueryArea=queryAreaIterator.next();
									List<Envelope> queryList=qt.query(currentQueryArea);
									if(queryList.size()!=0){
									result.add(new Tuple2<Envelope,ArrayList<Envelope>>(currentQueryArea,new ArrayList<Envelope>(queryList)));
									}
								}
								return result;
							}
							else
							{
								STRtree rt=new STRtree();
								Iterator<Envelope> targetIterator=t._2()._2().iterator();
								Iterator<Envelope> queryAreaIterator=t._2()._1().iterator();
								ArrayList<Tuple2<Envelope,ArrayList<Envelope>>> result=new ArrayList();
								while(targetIterator.hasNext())
								{
									Envelope currentTarget=targetIterator.next();
									rt.insert(currentTarget, currentTarget);
								}
								while(queryAreaIterator.hasNext())
								{
									Envelope currentQueryArea=queryAreaIterator.next();
									List<Envelope> queryList=rt.query(currentQueryArea);
									if(queryList.size()!=0){
									result.add(new Tuple2<Envelope,ArrayList<Envelope>>(currentQueryArea,new ArrayList<Envelope>(queryList)));
									}
								}
								return result;
							}
						
					}
			
				});
		//Delete the duplicate result
				JavaPairRDD<Envelope, ArrayList<Envelope>> aggregatedResult=queryResult.reduceByKey(new Function2<ArrayList<Envelope>,ArrayList<Envelope>,ArrayList<Envelope>>()
						{

							public ArrayList<Envelope> call(ArrayList<Envelope> v1,
									ArrayList<Envelope> v2) {
								ArrayList<Envelope> v3=v1;
								v3.addAll(v2);
								return v2;
							}
					
						});
				JavaPairRDD<Envelope,ArrayList<Envelope>> refinedResult=aggregatedResult.mapToPair(new PairFunction<Tuple2<Envelope,ArrayList<Envelope>>,Envelope,ArrayList<Envelope>>()
						{

							public Tuple2<Envelope, ArrayList<Envelope>> call(Tuple2<Envelope, ArrayList<Envelope>> v){
								ArrayList<Envelope> result=new ArrayList<Envelope>();
								Iterator<Envelope> targetIterator=v._2().iterator();
								for(int i=0;i<v._2().size();i++)
								{
									Integer duplicationFlag=0;
									Envelope currentPointi=v._2().get(i);
									for(int j=i+1;j<v._2().size();j++)
									{
										Envelope currentPointj=v._2().get(j);
										if(currentPointi.equals(currentPointj))
										{
											duplicationFlag=1;
										}
									}
									if(duplicationFlag==0)
									{
										result.add(currentPointi);
									}
								}
								return new Tuple2<Envelope,ArrayList<Envelope>>(v._1(),result);
							}
					
						});
				SpatialPairRDD<Envelope,ArrayList<Envelope>> result=new SpatialPairRDD<Envelope,ArrayList<Envelope>>(refinedResult);
		return result;
	}
	
	
	/**
	 * Spatial join query with index.
	 *
	 * @param rectangleRDD
	 *            the rectangle rdd
	 * @param GridNumberHorizontal
	 *            the grid number horizontal
	 * @param GridNumberVertical
	 *            the grid number vertical
	 * @param Index
	 *            the index
	 * @return the spatial pair rdd
	 */
	public SpatialPairRDD<Envelope, ArrayList<Point>> SpatialJoinQueryWithIndex(PointRDD pointRDD, RectangleRDD rectangleRDD,
			Integer GridNumberHorizontal, Integer GridNumberVertical, String Index) {
		// Find the border of both of the two datasets---------------
		// final Integer condition=Condition;
		final String index = Index;
		// condition=0 means only consider fully contain in query, condition=1
		// means consider full contain and partial contain(overlap).
		// QueryAreaSet min/max longitude and latitude
		Envelope QueryWindowSetBoundary = rectangleRDD.boundary();
		// TargetSet min/max longitude and latitude
		Envelope TargetSetBoundary = pointRDD.boundary();
		Envelope boundary = QueryWindowSetBoundary;
		// Integer
		// currentPartitionsTargetSet=this.pointRDD.partitions().size()/2;
		// Integer
		// currentPartitionQuerySet=rectangleRDD.getRectangleRDD().partitions().size();
		// Border found
		JavaRDD<Point> TargetPreFiltered;// =this.pointRDD.filter(new
											// PointPreFilter(boundary));
		JavaRDD<Envelope> QueryAreaPreFiltered;// =rectangleRDD.getRectangleRDD().filter(new
												// RectanglePreFilter(boundary));
		if (QueryWindowSetBoundary.contains(TargetSetBoundary)) {
			boundary = TargetSetBoundary;
			// TargetPreFiltered=this.pointRDD;
			// QueryAreaPreFiltered=rectangleRDD.getRectangleRDD().filter(new
			// RectanglePreFilter(boundary));
		} else if (TargetSetBoundary.contains(QueryWindowSetBoundary)) {
			boundary = QueryWindowSetBoundary;
			// TargetPreFiltered=this.pointRDD.filter(new
			// PointPreFilter(boundary));//.repartition(currentPartitionsTargetSet);
			// QueryAreaPreFiltered=rectangleRDD.getRectangleRDD();
		} else if (QueryWindowSetBoundary.intersects(TargetSetBoundary)) {
			boundary = QueryWindowSetBoundary.intersection(TargetSetBoundary);
			// TargetPreFiltered=this.pointRDD.filter(new
			// PointPreFilter(boundary));//.repartition(currentPartitionsTargetSet);
			// QueryAreaPreFiltered=rectangleRDD.getRectangleRDD().filter(new
			// RectanglePreFilter(boundary));
		} else {
			System.out.println("Two input sets are not overlapped");
			return null;
		}
		// JavaRDD<Point> TargetPreFiltered=this.pointRDD.filter(new
		// PointPreFilter(boundary));
		// JavaRDD<Envelope>
		// QueryAreaPreFiltered=rectangleRDD.getRectangleRDD().filter(new
		// RectanglePreFilter(boundary));
		// Build Grid file-------------------
		Double[] gridHorizontalBorder = new Double[GridNumberHorizontal + 1];
		Double[] gridVerticalBorder = new Double[GridNumberVertical + 1];
		double LongitudeIncrement = (boundary.getMaxX() - boundary.getMinX()) / GridNumberHorizontal;
		double LatitudeIncrement = (boundary.getMaxY() - boundary.getMinY()) / GridNumberVertical;
		for (int i = 0; i < GridNumberHorizontal + 1; i++) {
			gridHorizontalBorder[i] = boundary.getMinX() + LongitudeIncrement * i;
		}
		for (int i = 0; i < GridNumberVertical + 1; i++) {
			gridVerticalBorder[i] = boundary.getMinY() + LatitudeIncrement * i;
		}
		// Assign grid ID to both of the two dataset---------------------
		JavaPairRDD<Integer, Point> TargetSetWithIDtemp = pointRDD.getPointRDD()
				.mapPartitionsToPair(new PartitionAssignGridPoint(GridNumberHorizontal, GridNumberVertical,
						gridHorizontalBorder, gridVerticalBorder));
		JavaPairRDD<Integer, Envelope> QueryAreaSetWithIDtemp = rectangleRDD.getRectangleRDD()
				.mapPartitionsToPair(new PartitionAssignGridRectangle(GridNumberHorizontal, GridNumberVertical,
						gridHorizontalBorder, gridVerticalBorder));
		// Remove cache from memory
		pointRDD.getPointRDD().unpersist();
		rectangleRDD.getRectangleRDD().unpersist();
		JavaPairRDD<Integer, Point> TargetSetWithID = TargetSetWithIDtemp;// .repartition(TargetSetWithIDtemp.partitions().size()*2);
		JavaPairRDD<Integer, Envelope> QueryAreaSetWithID = QueryAreaSetWithIDtemp;// .repartition(TargetSetWithIDtemp.partitions().size()*2);
		// Join two dataset
		// JavaPairRDD<Integer,Tuple2<Envelope,Point>>
		// joinSet1=QueryAreaSetWithID.join(TargetSetWithID,
		// TargetSetWithIDtemp.partitions().size()*2);
		JavaPairRDD<Integer, Tuple2<Iterable<Envelope>, Iterable<Point>>> cogroupSet = QueryAreaSetWithID
				.cogroup(TargetSetWithID, TargetSetWithIDtemp.partitions().size() * 2);

		JavaPairRDD<Envelope, ArrayList<Point>> queryResult = cogroupSet.flatMapToPair(
				new PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<Envelope>, Iterable<Point>>>, Envelope, ArrayList<Point>>() {

					public Iterable<Tuple2<Envelope, ArrayList<Point>>> call(
							Tuple2<Integer, Tuple2<Iterable<Envelope>, Iterable<Point>>> t) {

						if (index == "quadtree") {
							Quadtree qt = new Quadtree();
							Iterator<Point> targetIterator = t._2()._2().iterator();
							Iterator<Envelope> queryAreaIterator = t._2()._1().iterator();
							ArrayList<Tuple2<Envelope, ArrayList<Point>>> result = new ArrayList();
							while (targetIterator.hasNext()) {
								Point currentTarget = targetIterator.next();
								qt.insert(currentTarget.getEnvelopeInternal(), currentTarget);
							}
							while (queryAreaIterator.hasNext()) {
								Envelope currentQueryArea = queryAreaIterator.next();
								List<Point> queryList = qt.query(currentQueryArea);
								if (queryList.size() != 0) {
									result.add(new Tuple2<Envelope, ArrayList<Point>>(currentQueryArea,
											new ArrayList<Point>(queryList)));
								}
							}
							return result;
						} else {
							STRtree rt = new STRtree();
							Iterator<Point> targetIterator = t._2()._2().iterator();
							Iterator<Envelope> queryAreaIterator = t._2()._1().iterator();
							ArrayList<Tuple2<Envelope, ArrayList<Point>>> result = new ArrayList();
							while (targetIterator.hasNext()) {
								Point currentTarget = targetIterator.next();
								rt.insert(currentTarget.getEnvelopeInternal(), currentTarget);
							}
							while (queryAreaIterator.hasNext()) {
								Envelope currentQueryArea = queryAreaIterator.next();
								List<Point> queryList = rt.query(currentQueryArea);
								if (queryList.size() != 0) {
									result.add(new Tuple2<Envelope, ArrayList<Point>>(currentQueryArea,
											new ArrayList<Point>(queryList)));
								}
							}
							return result;
						}

					}

				});
		// Delete the duplicate result
		JavaPairRDD<Envelope, ArrayList<Point>> aggregatedResult = queryResult
				.reduceByKey(new Function2<ArrayList<Point>, ArrayList<Point>, ArrayList<Point>>() {

					public ArrayList<Point> call(ArrayList<Point> v1, ArrayList<Point> v2) {
						ArrayList<Point> v3 = v1;
						v3.addAll(v2);
						return v2;
					}

				});
		JavaPairRDD<Envelope, ArrayList<Point>> refinedResult = aggregatedResult
				.mapToPair(new PairFunction<Tuple2<Envelope, ArrayList<Point>>, Envelope, ArrayList<Point>>() {

					public Tuple2<Envelope, ArrayList<Point>> call(Tuple2<Envelope, ArrayList<Point>> v) {
						ArrayList<Point> result = new ArrayList<Point>();
						Iterator<Point> targetIterator = v._2().iterator();
						for (int i = 0; i < v._2().size(); i++) {
							Integer duplicationFlag = 0;
							Point currentPointi = v._2().get(i);
							for (int j = i + 1; j < v._2().size(); j++) {
								Point currentPointj = v._2().get(j);
								if (currentPointi.equals(currentPointj)) {
									duplicationFlag = 1;
								}
							}
							if (duplicationFlag == 0) {
								result.add(currentPointi);
							}
						}
						return new Tuple2<Envelope, ArrayList<Point>>(v._1(), result);
					}

				});
		SpatialPairRDD<Envelope, ArrayList<Point>> result = new SpatialPairRDD<Envelope, ArrayList<Point>>(
				refinedResult);
		return result;
	}

	/**
	 * Spatial join query with index.
	 *
	 * @param circleRDD
	 *            the circle rdd
	 * @param GridNumberHorizontal
	 *            the grid number horizontal
	 * @param GridNumberVertical
	 *            the grid number vertical
	 * @param Index
	 *            the index
	 * @return the spatial pair rdd
	 */
	public SpatialPairRDD<Point, ArrayList<Point>> SpatialJoinQueryWithIndex(PointRDD pointRDD, CircleRDD circleRDD,
			Integer GridNumberHorizontal, Integer GridNumberVertical, String Index) {
		// Find the border of both of the two datasets---------------
		// condition=0 means only consider fully contain in query, condition=1
		// means consider full contain and partial contain(overlap).
		// QueryAreaSet min/max longitude and latitude
		final String index = Index;
		Envelope QueryWindowSetBoundary = circleRDD.boundary();
		// TargetSet min/max longitude and latitude
		Envelope TargetSetBoundary = pointRDD.boundary();
		Envelope boundary;
		// Border found
		JavaRDD<Point> TargetPreFiltered;
		JavaRDD<Circle> QueryAreaPreFiltered;
		// Integer
		// currentPartitionsTargetSet=this.pointRDD.partitions().size()/2;
		// Integer
		// currentPartitionQuerySet=circleRDD.getCircleRDD().partitions().size();
		if (QueryWindowSetBoundary.contains(TargetSetBoundary)) {
			boundary = TargetSetBoundary;
			// TargetPreFiltered=this.pointRDD;
			// QueryAreaPreFiltered=circleRDD.getCircleRDD().filter(new
			// CirclePreFilter(boundary));
		} else if (TargetSetBoundary.contains(QueryWindowSetBoundary)) {
			boundary = QueryWindowSetBoundary;
			// TargetPreFiltered=this.pointRDD.filter(new
			// PointPreFilter(boundary));//.repartition(currentPartitionsTargetSet);
			// QueryAreaPreFiltered=circleRDD.getCircleRDD();
		} else if (QueryWindowSetBoundary.intersects(TargetSetBoundary)) {
			boundary = QueryWindowSetBoundary.intersection(TargetSetBoundary);
			// TargetPreFiltered=this.pointRDD.filter(new
			// PointPreFilter(boundary));//.repartition(currentPartitionsTargetSet);
			// QueryAreaPreFiltered=circleRDD.getCircleRDD().filter(new
			// CirclePreFilter(boundary));
		} else {
			System.out.println("Two input sets are not overlapped");
			return null;
		}

		// Build Grid file-------------------
		Double[] gridHorizontalBorder = new Double[GridNumberHorizontal + 1];
		Double[] gridVerticalBorder = new Double[GridNumberVertical + 1];
		double LongitudeIncrement = (boundary.getMaxX() - boundary.getMinX()) / GridNumberHorizontal;
		double LatitudeIncrement = (boundary.getMaxY() - boundary.getMinY()) / GridNumberVertical;
		for (int i = 0; i < GridNumberHorizontal + 1; i++) {
			gridHorizontalBorder[i] = boundary.getMinX() + LongitudeIncrement * i;
		}
		for (int i = 0; i < GridNumberVertical + 1; i++) {
			gridVerticalBorder[i] = boundary.getMinY() + LatitudeIncrement * i;
		}
		// Assign grid ID to both of the two dataset---------------------

		JavaPairRDD<Integer, Point> TargetSetWithIDtemp = pointRDD.getPointRDD()
				.mapPartitionsToPair(new PartitionAssignGridPoint(GridNumberHorizontal, GridNumberVertical,
						gridHorizontalBorder, gridVerticalBorder));
		JavaPairRDD<Integer, Circle> QueryAreaSetWithIDtemp = circleRDD.getCircleRDD()
				.mapPartitionsToPair(new PartitionAssignGridCircle(GridNumberHorizontal, GridNumberVertical,
						gridHorizontalBorder, gridVerticalBorder));
		pointRDD.getPointRDD().unpersist();
		circleRDD.getCircleRDD().unpersist();
		JavaPairRDD<Integer, Point> TargetSetWithID = TargetSetWithIDtemp;// .repartition(TargetSetWithIDtemp.partitions().size()*2);
		JavaPairRDD<Integer, Circle> QueryAreaSetWithID = QueryAreaSetWithIDtemp;// .repartition(TargetSetWithIDtemp.partitions().size()*2);
		// Join two dataset
		JavaPairRDD<Integer, Tuple2<Iterable<Circle>, Iterable<Point>>> cogroupSet = QueryAreaSetWithID
				.cogroup(TargetSetWithID, TargetSetWithIDtemp.partitions().size() * 2);

		JavaPairRDD<Circle, ArrayList<Point>> queryResult = cogroupSet.flatMapToPair(
				new PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<Circle>, Iterable<Point>>>, Circle, ArrayList<Point>>() {

					public Iterable<Tuple2<Circle, ArrayList<Point>>> call(
							Tuple2<Integer, Tuple2<Iterable<Circle>, Iterable<Point>>> t) {

						if (index == "quadtree") {
							Quadtree qt = new Quadtree();
							Iterator<Point> targetIterator = t._2()._2().iterator();
							Iterator<Circle> queryAreaIterator = t._2()._1().iterator();
							ArrayList<Tuple2<Circle, ArrayList<Point>>> result = new ArrayList();
							while (targetIterator.hasNext()) {
								Point currentTarget = targetIterator.next();
								qt.insert(currentTarget.getEnvelopeInternal(), currentTarget);
							}
							while (queryAreaIterator.hasNext()) {
								Circle currentQueryArea = queryAreaIterator.next();
								List<Point> queryList = qt.query(currentQueryArea.getMBR());
								if (queryList.size() != 0) {
									result.add(new Tuple2<Circle, ArrayList<Point>>(currentQueryArea,
											new ArrayList<Point>(queryList)));
								}
							}
							return result;
						} else {
							STRtree rt = new STRtree();
							Iterator<Point> targetIterator = t._2()._2().iterator();
							Iterator<Circle> queryAreaIterator = t._2()._1().iterator();
							ArrayList<Tuple2<Circle, ArrayList<Point>>> result = new ArrayList();
							while (targetIterator.hasNext()) {
								Point currentTarget = targetIterator.next();
								rt.insert(currentTarget.getEnvelopeInternal(), currentTarget);
							}
							while (queryAreaIterator.hasNext()) {
								Circle currentQueryArea = queryAreaIterator.next();
								List<Point> queryList = rt.query(currentQueryArea.getMBR());
								if (queryList.size() != 0) {
									result.add(new Tuple2<Circle, ArrayList<Point>>(currentQueryArea,
											new ArrayList<Point>(queryList)));
								}
							}
							return result;
						}

					}

				});
		// Delete the duplicate result
		JavaPairRDD<Circle, ArrayList<Point>> aggregatedResult = queryResult
				.reduceByKey(new Function2<ArrayList<Point>, ArrayList<Point>, ArrayList<Point>>() {

					public ArrayList<Point> call(ArrayList<Point> v1, ArrayList<Point> v2) {
						ArrayList<Point> v3 = v1;
						v3.addAll(v2);
						return v2;
					}

				});
		JavaPairRDD<Point, ArrayList<Point>> refinedResult = aggregatedResult
				.mapToPair(new PairFunction<Tuple2<Circle, ArrayList<Point>>, Point, ArrayList<Point>>() {

					public Tuple2<Point, ArrayList<Point>> call(Tuple2<Circle, ArrayList<Point>> v) {
						ArrayList<Point> result = new ArrayList<Point>();
						Iterator<Point> targetIterator = v._2().iterator();
						for (int i = 0; i < v._2().size(); i++) {
							Integer duplicationFlag = 0;
							Point currentPointi = v._2().get(i);
							for (int j = i + 1; j < v._2().size(); j++) {
								Point currentPointj = v._2().get(j);
								if (currentPointi.equals(currentPointj)) {
									duplicationFlag = 1;
								}
							}
							if (duplicationFlag == 0) {
								result.add(currentPointi);
							}
						}
						return new Tuple2<Point, ArrayList<Point>>(v._1().getCentre(), result);
					}

				});
		SpatialPairRDD<Point, ArrayList<Point>> result = new SpatialPairRDD<Point, ArrayList<Point>>(refinedResult);
		return result;
	}
	
}
