package GeoSpark;

import java.io.Serializable;
import java.util.Comparator;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;


public class ComparatorCollection {

}
class PointXComparator implements Comparator<Point>, Serializable {
    
	 public int compare(Point point1, Point point2) {
	    if(point1.getX()>point2.getX())
	    {
	    	return 1;
	    }
	    else if (point1.getX()<point2.getX())
	    {
	    	return -1;
	    }
	    else return 0;
	    }
}
class PointYComparator implements Comparator<Point>, Serializable {
 
	 public int compare(Point point1, Point point2) {
	    if(point1.getY()>point2.getY())
	    {
	    	return 1;
	    }
	    else if (point1.getY()<point2.getY())
	    {
	    	return -1;
	    }
	    else return 0;
	    }
}
class RectangleXMinComparator implements Comparator<Envelope>, Serializable {
  
	 public int compare(Envelope envelope1, Envelope envelope2) {
	    if(envelope1.getMinX()>envelope2.getMinX())
	    {
	    	return 1;
	    }
	    else if (envelope1.getMinX()<envelope2.getMinX())
	    {
	    	return -1;
	    }
	    else return 0;
	    }
}
class RectangleYMinComparator implements Comparator<Envelope>, Serializable {
	  
	 public int compare(Envelope envelope1, Envelope envelope2) {
	    if(envelope1.getMinY()>envelope2.getMinY())
	    {
	    	return 1;
	    }
	    else if (envelope1.getMinY()<envelope2.getMinY())
	    {
	    	return -1;
	    }
	    else return 0;
	    }
}
class RectangleXMaxComparator implements Comparator<Envelope>, Serializable {
	  
	 public int compare(Envelope envelope1, Envelope envelope2) {
	    if(envelope1.getMaxX()>envelope2.getMaxX())
	    {
	    	return 1;
	    }
	    else if (envelope1.getMaxX()<envelope2.getMaxX())
	    {
	    	return -1;
	    }
	    else return 0;
	    }
}
class RectangleYMaxComparator implements Comparator<Envelope>, Serializable {
	  
	 public int compare(Envelope envelope1, Envelope envelope2) {
	    if(envelope1.getMaxY()>envelope2.getMaxY())
	    {
	    	return 1;
	    }
	    else if (envelope1.getMaxY()<envelope2.getMaxY())
	    {
	    	return -1;
	    }
	    else return 0;
	    }
}
class PolygonXMinComparator implements Comparator<Polygon>, Serializable
{
	
	public int compare(Polygon polygon1, Polygon polygon2) {
	    if(polygon1.getEnvelopeInternal().getMinX()>polygon2.getEnvelopeInternal().getMinX())
	    {
	    	return 1;
	    }
	    else if (polygon1.getEnvelopeInternal().getMinX()<polygon2.getEnvelopeInternal().getMinX())
	    {
	    	return -1;
	    }
	    else return 0;
	    }
}
class PolygonYMinComparator implements Comparator<Polygon>, Serializable
{
	
	public int compare(Polygon polygon1, Polygon polygon2) {
	    if(polygon1.getEnvelopeInternal().getMinY()>polygon2.getEnvelopeInternal().getMinY())
	    {
	    	return 1;
	    }
	    else if (polygon1.getEnvelopeInternal().getMinY()<polygon2.getEnvelopeInternal().getMinY())
	    {
	    	return -1;
	    }
	    else return 0;
	    }
}
class PolygonXMaxComparator implements Comparator<Polygon>, Serializable
{
	
	public int compare(Polygon polygon1, Polygon polygon2) {
	    if(polygon1.getEnvelopeInternal().getMaxX()>polygon2.getEnvelopeInternal().getMaxX())
	    {
	    	return 1;
	    }
	    else if (polygon1.getEnvelopeInternal().getMaxX()<polygon2.getEnvelopeInternal().getMaxX())
	    {
	    	return -1;
	    }
	    else return 0;
	    }
}
class PolygonYMaxComparator implements Comparator<Polygon>, Serializable
{
	
	public int compare(Polygon polygon1, Polygon polygon2) {
	    if(polygon1.getEnvelopeInternal().getMaxY()>polygon2.getEnvelopeInternal().getMaxY())
	    {
	    	return 1;
	    }
	    else if (polygon1.getEnvelopeInternal().getMaxY()<polygon2.getEnvelopeInternal().getMaxY())
	    {
	    	return -1;
	    }
	    else return 0;
	    }
}
class CircleXMinComparator implements Comparator<Circle>, Serializable {
	  
	 public int compare(Circle circle1, Circle circle2) {
		 Envelope envelope1=circle1.getMBR();
		 Envelope envelope2=circle1.getMBR();
	    if(envelope1.getMinX()>envelope2.getMinX())
	    {
	    	return 1;
	    }
	    else if (envelope1.getMinX()<envelope2.getMinX())
	    {
	    	return -1;
	    }
	    else return 0;
	    }
}
class CircleYMinComparator implements Comparator<Circle>, Serializable {
	  
	 public int compare(Circle circle1, Circle circle2) {
		 Envelope envelope1=circle1.getMBR();
		 Envelope envelope2=circle1.getMBR();
	    if(envelope1.getMinY()>envelope2.getMinY())
	    {
	    	return 1;
	    }
	    else if (envelope1.getMinY()<envelope2.getMinY())
	    {
	    	return -1;
	    }
	    else return 0;
	    }
}
class CircleXMaxComparator implements Comparator<Circle>, Serializable {
	  
	 public int compare(Circle circle1, Circle circle2) {
		 Envelope envelope1=circle1.getMBR();
		 Envelope envelope2=circle1.getMBR();
	    if(envelope1.getMaxX()>envelope2.getMaxX())
	    {
	    	return 1;
	    }
	    else if (envelope1.getMaxX()<envelope2.getMaxX())
	    {
	    	return -1;
	    }
	    else return 0;
	    }
}
class CircleYMaxComparator implements Comparator<Circle>, Serializable {
	  
	 public int compare(Circle circle1, Circle circle2) {
		 Envelope envelope1=circle1.getMBR();
		 Envelope envelope2=circle1.getMBR();
	    if(envelope1.getMaxY()>envelope2.getMaxY())
	    {
	    	return 1;
	    }
	    else if (envelope1.getMaxY()<envelope2.getMaxY())
	    {
	    	return -1;
	    }
	    else return 0;
	    }
}