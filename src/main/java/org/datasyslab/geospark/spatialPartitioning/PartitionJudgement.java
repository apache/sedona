package org.datasyslab.geospark.spatialPartitioning;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

import org.datasyslab.geospark.geometryObjects.EnvelopeWithGrid;

import scala.Tuple2;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

public class PartitionJudgement implements Serializable{
	public PartitionJudgement()
	{
		
	}
	public static Tuple2<Integer, Point> getPartitionID(HashSet<EnvelopeWithGrid> grid,Point point)
	{
		//HashSet<Tuple2<Integer, Point>> result = new HashSet<Tuple2<Integer, Point>>();
		Iterator<EnvelopeWithGrid> iteratorGrid=grid.iterator();
		EnvelopeWithGrid gridInstance=null;
		Random random = new Random();
		int randomID=grid.size();
		while(iteratorGrid.hasNext())
		{
			gridInstance=iteratorGrid.next();
			if(gridInstance.contains(point.getCoordinate()))
			{
				return new Tuple2<Integer, Point>(gridInstance.grid,point);
				 
			}
		}
		//We missed this object. Copy this object to all partitions. Normally, no point should reach here if use equal grid.
		/*iteratorGrid=grid.iterator();
		while(iteratorGrid.hasNext())
		{
			EnvelopeWithGrid gridInstance=iteratorGrid.next();
			result.add(new Tuple2<Integer, Point>(gridInstance.grid,point));
		}*/
		
		//We missed this object. Copy this object to all partitions. Normally, no point should reach here if use equal grid.
		//We partition these missed objects into random additional partitions and distribute them.
		if(grid.size()>10)
		{
			randomID=random.nextInt(grid.size()/10) + grid.size();
		}
		return new Tuple2<Integer,Point>(randomID,point);
	}
	
	public static HashSet<Tuple2<Integer, Envelope>> getPartitionID(HashSet<EnvelopeWithGrid> grid,Envelope envelope)
	{
		HashSet<Tuple2<Integer, Envelope>> result = new HashSet<Tuple2<Integer, Envelope>>();
		Iterator<EnvelopeWithGrid> iteratorGrid=grid.iterator();
		EnvelopeWithGrid gridInstance=null;
		Random random = new Random();
		int randomID=grid.size();
		while(iteratorGrid.hasNext())
		{
			gridInstance=iteratorGrid.next();
			if(gridInstance.intersects(envelope) || gridInstance.contains(envelope))
			{
				result.add(new Tuple2<Integer, Envelope>(gridInstance.grid, envelope));
			}
		}
		if(result.size()==0)
		{
			//We missed this object. Copy this object to all partitions
			//We partition these missed objects into random additional partitions and distribute them.
			if(grid.size()>10)
			{
				randomID=random.nextInt(grid.size()/10) + grid.size();
			}
			result.add(new Tuple2<Integer, Envelope>(randomID,envelope));
		}
		return result;
	}
	
	public static HashSet<Tuple2<Integer, Polygon>> getPartitionID(HashSet<EnvelopeWithGrid> grid,Polygon polygon)
	{
		HashSet<Tuple2<Integer, Polygon>> result = new HashSet<Tuple2<Integer, Polygon>>();
		Iterator<EnvelopeWithGrid> iteratorGrid=grid.iterator();
		EnvelopeWithGrid gridInstance=null;
		Random random = new Random();
		int randomID=grid.size();
		while(iteratorGrid.hasNext())
		{
			gridInstance=iteratorGrid.next();
			if(gridInstance.intersects(polygon.getEnvelopeInternal())||gridInstance.contains(polygon.getEnvelopeInternal()))
			{
				result.add(new Tuple2<Integer, Polygon>(gridInstance.grid, polygon));
			}
		}
		if(result.size()==0)
		{
			//We missed this object. Copy this object to all partitions
			//We partition these missed objects into random additional partitions and distribute them.
			if(grid.size()>10)
			{
				randomID=random.nextInt(grid.size()/10) + grid.size();
			}
			result.add(new Tuple2<Integer, Polygon>(randomID,polygon));
		}
		return result;
	}
	
}
