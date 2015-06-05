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
	public Double[] boundary()
	{
		Double[] boundary = new Double[4]; 
		Double minLongtitude1=this.circleRDD.min(new CircleXMinComparator()).getMBR().getMinX();
		Double maxLongtitude1=this.circleRDD.max(new CircleXMinComparator()).getMBR().getMinX();
		Double minLatitude1=this.circleRDD.min(new CircleYMinComparator()).getMBR().getMinY();
		Double maxLatitude1=this.circleRDD.max(new CircleYMinComparator()).getMBR().getMinY();
		Double minLongtitude2=this.circleRDD.min(new CircleXMaxComparator()).getMBR().getMaxX();
		Double maxLongtitude2=this.circleRDD.max(new CircleXMaxComparator()).getMBR().getMaxX();
		Double minLatitude2=this.circleRDD.min(new CircleYMaxComparator()).getMBR().getMaxY();
		Double maxLatitude2=this.circleRDD.max(new CircleYMaxComparator()).getMBR().getMaxY();
		if(minLongtitude1<minLongtitude2)
		{
			boundary[0]=minLongtitude1;
		}
		else
		{
			boundary[0]=minLongtitude2;
		}
		if(minLatitude1<minLatitude2)
		{
			boundary[1]=minLatitude1;
		}
		else
		{
			boundary[1]=minLatitude2;
		}
		if(maxLongtitude1>maxLongtitude2)
		{
			boundary[2]=maxLongtitude1;
		}
		else
		{
			boundary[2]=maxLongtitude2;
		}
		if(maxLatitude1>maxLatitude2)
		{
			boundary[3]=maxLatitude1;
		}
		else
		{
			boundary[3]=maxLatitude2;
		}
		return boundary;
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
