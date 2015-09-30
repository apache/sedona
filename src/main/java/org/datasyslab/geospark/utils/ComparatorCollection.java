package org.datasyslab.geospark.utils;

import java.io.Serializable;
import java.util.Comparator;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

import org.datasyslab.geospark.gemotryObjects.*;

public class ComparatorCollection {
	
	public static GemotryComparator createComparator(String gemotryType, String axis){
		GemotryComparator comp = null;
		switch(gemotryType.toUpperCase()) {
		case "POINT":
			if(axis.toUpperCase().equals("X"))
				comp = new PointXComparator();
			break;
		}
		return comp;
	}
}
class PointYComparator extends GemotryComparator implements Comparator<Point>, Serializable {
 
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
class RectangleXMinComparator extends GemotryComparator implements Comparator<Envelope>, Serializable {
  
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
class RectangleYMinComparator extends GemotryComparator implements Comparator<Envelope>, Serializable {
	  
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
class RectangleXMaxComparator extends GemotryComparator implements Comparator<Envelope>, Serializable {
	  
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
class RectangleYMaxComparator extends GemotryComparator implements Comparator<Envelope>, Serializable {
	  
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
class PolygonXMinComparator extends GemotryComparator implements Comparator<Polygon>, Serializable
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
class PolygonYMinComparator extends GemotryComparator implements Comparator<Polygon>, Serializable
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
class PolygonXMaxComparator extends GemotryComparator implements Comparator<Polygon>, Serializable
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
class PolygonYMaxComparator extends GemotryComparator implements Comparator<Polygon>, Serializable
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
class CircleXMinComparator extends GemotryComparator implements Comparator<Circle>, Serializable {
	  
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
class CircleYMinComparator extends GemotryComparator implements Comparator<Circle>, Serializable {
	  
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
class CircleXMaxComparator extends GemotryComparator implements Comparator<Circle>, Serializable {
	  
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
class CircleYMaxComparator extends GemotryComparator implements Comparator<Circle>, Serializable {
	  
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