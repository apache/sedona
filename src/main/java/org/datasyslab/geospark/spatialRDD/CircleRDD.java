package org.datasyslab.geospark.spatialRDD;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;

import org.datasyslab.geospark.geometryObjects.*;
import org.datasyslab.geospark.utils.*;;

// TODO: Auto-generated Javadoc

/**
 * The Class CircleRDD. It accommodates Circle object. 
 * @author Arizona State University DataSystems Lab
 *
 */
public class CircleRDD implements Serializable {

	/** The circle rdd. */
	private JavaRDD<Circle> circleRDD;


	/**
	 * Instantiates a new circle rdd.
	 *
	 * @param circleRDD
	 *            the circle rdd
	 */
	public CircleRDD(JavaRDD<Circle> circleRDD) {
		this.setCircleRDD(circleRDD.cache());
	}

	/**
	 * Instantiates a new circle rdd. Each Circle in this RDD uses one Point in PointRDD as the center and the specified radius as the radius
	 *
	 * @param pointRDD
	 *            the point rdd
	 * @param Radius
	 *            the radius
	 */
	public CircleRDD(PointRDD pointRDD, Double Radius) {
		final Double radius = Radius;
		this.circleRDD = pointRDD.getRawPointRDD().map(new Function<Point, Circle>() {

			public Circle call(Point v1) {

				return new Circle(v1, radius);
			}

		});
	}

	/**
	 * Get the circle rdd.
	 *
	 * @return the circle rdd
	 */
	public JavaRDD<Circle> getCircleRDD() {
		return circleRDD;
	}

	/**
	 * Set the circle rdd.
	 *
	 * @param circleRDD
	 *            the new circle rdd
	 */
	public void setCircleRDD(JavaRDD<Circle> circleRDD) {
		this.circleRDD = circleRDD;
	}

    /**
     * Return RectangleRDD version of the CircleRDD. Each record in RectangleRDD is the Minimum bounding rectangle of the corresponding Circle
     *
     * @return the rectangle rdd
     */
	public RectangleRDD MinimumBoundingRectangle() {
		return new RectangleRDD(this.getCircleRDD().map(new Function<Circle, Envelope>() {

			public Envelope call(Circle v1) {

				return v1.getMBR();
			}

		}));
	}

	/**
	 * Return the boundary of the entire SpatialRDD in terms of an envelope format
	 * @return the envelope
	 */
	public Envelope boundary() {
		Double[] boundary = new Double[4];
		Double minLongtitude1 = this.circleRDD
				.min((CircleXMinComparator) GeometryComparatorFactory.createComparator("circle", "x", "min")).getMBR()
				.getMinX();
		Double maxLongtitude1 = this.circleRDD
				.max((CircleXMinComparator) GeometryComparatorFactory.createComparator("circle", "x", "min")).getMBR()
				.getMinX();
		Double minLatitude1 = this.circleRDD
				.min((CircleYMinComparator) GeometryComparatorFactory.createComparator("circle", "y", "min")).getMBR()
				.getMinY();
		Double maxLatitude1 = this.circleRDD
				.max((CircleYMinComparator) GeometryComparatorFactory.createComparator("circle", "y", "min")).getMBR()
				.getMinY();
		Double minLongtitude2 = this.circleRDD
				.min((CircleXMaxComparator) GeometryComparatorFactory.createComparator("circle", "x", "max")).getMBR()
				.getMaxX();
		Double maxLongtitude2 = this.circleRDD
				.max((CircleXMaxComparator) GeometryComparatorFactory.createComparator("circle", "x", "max")).getMBR()
				.getMaxX();
		Double minLatitude2 = this.circleRDD
				.min((CircleYMaxComparator) GeometryComparatorFactory.createComparator("circle", "y", "max")).getMBR()
				.getMaxY();
		Double maxLatitude2 = this.circleRDD
				.max((CircleYMaxComparator) GeometryComparatorFactory.createComparator("circle", "y", "max")).getMBR()
				.getMaxY();
		if (minLongtitude1 < minLongtitude2) {
			boundary[0] = minLongtitude1;
		} else {
			boundary[0] = minLongtitude2;
		}
		if (minLatitude1 < minLatitude2) {
			boundary[1] = minLatitude1;
		} else {
			boundary[1] = minLatitude2;
		}
		if (maxLongtitude1 > maxLongtitude2) {
			boundary[2] = maxLongtitude1;
		} else {
			boundary[2] = maxLongtitude2;
		}
		if (maxLatitude1 > maxLatitude2) {
			boundary[3] = maxLatitude1;
		} else {
			boundary[3] = maxLatitude2;
		}
		return new Envelope(boundary[0], boundary[2], boundary[1], boundary[3]);
	}

	/**
	 * Return the PointRDD which is used to create this CircleRDD.
	 *
	 * @return the center point of each circle in the RDD
	 */
	public PointRDD Center() {
		return new PointRDD(this.getCircleRDD().map(new Function<Circle, Point>() {

			public Point call(Circle v1) {

				return v1.getCenter();
			}

		}));
	}
}
