package org.datasyslab.geospark.utils;

import com.vividsolutions.jts.geom.Envelope;
import org.datasyslab.geospark.gemotryObjects.Circle;
import java.io.Serializable;
import java.util.Comparator;

/**
 * Created by jinxuanw on 9/29/15.
 */
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
