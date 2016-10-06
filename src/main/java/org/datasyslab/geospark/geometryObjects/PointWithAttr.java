package org.datasyslab.geospark.geometryObjects;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import java.io.Serializable;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.PrecisionModel;

public class PointWithAttr extends Point implements Serializable{
	
	public PointWithAttr(Coordinate coordinate, PrecisionModel precisionModel, int SRID) {
		super(coordinate, precisionModel, SRID);
		// TODO Auto-generated constructor stub
	}
	
	public String headPart;
	public String tailPart;
	

}
