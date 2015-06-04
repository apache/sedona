package GeoSpark;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;

public class CircleRDD implements Serializable{
	private JavaRDD<Circle> circleRDD;
	public CircleRDD(JavaRDD<Circle> circleRDD)
	{
		this.setCircleRDD(circleRDD.cache());
	}
	public CircleRDD(PointRDD pointRDD, Double Radius)
	{
		final Double radius=Radius;
		this.circleRDD=pointRDD.getPointRDD().map(new Function<Point,Circle>()
				{

					public Circle call(Point v1) {
						
						return new Circle(v1,radius);
					}
			
				});
	}
	public JavaRDD<Circle> getCircleRDD() {
		return circleRDD;
	}

	public void setCircleRDD(JavaRDD<Circle> circleRDD) {
		this.circleRDD = circleRDD;
	}
	public RectangleRDD MinimumBoundingRectangle()
	{
		return new RectangleRDD(this.getCircleRDD().map(new Function<Circle,Envelope>()
				{

					public Envelope call(Circle v1) {
						
						return v1.getMBR();
					}
					
				}));
	}
	public PointRDD Centre()
	{
		return new PointRDD(this.getCircleRDD().map(new Function<Circle,Point>()
				{

					public Point call(Circle v1) {
						
						return v1.getCentre();
					}
					
				}));
	}
}