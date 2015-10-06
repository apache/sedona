package org.datasyslab.geospark.partition;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;
import org.datasyslab.geospark.gemotryObjects.*;

import com.vividsolutions.jts.geom.Envelope;

public class PartitionAssignGridCircle implements PairFlatMapFunction<java.util.Iterator<Circle>,Integer,Circle>, Serializable {
	int gridNumberHorizontal;
	int gridNumberVertical;
	Double[] gridHorizontalBorder;
	Double[] gridVerticalBorder;
	public PartitionAssignGridCircle(int gridNumberHorizontal, int gridNumberVertical, Double[] gridHorizontalBorder, Double[] gridVerticalBorder) 
	{
		this.gridNumberHorizontal=gridNumberHorizontal;
		this.gridNumberVertical=gridNumberVertical;
		this.gridHorizontalBorder=gridHorizontalBorder;
		this.gridVerticalBorder=gridVerticalBorder;
	}
	public Iterable<Tuple2<Integer, Circle>> call(Iterator<Circle> s) throws Exception 	
	{
		//int id=-1;
		ArrayList<Tuple2<Integer, Circle>> list=new ArrayList<Tuple2<Integer, Circle>>();
		
		while(s.hasNext())
		{
			Circle currentCircle=s.next();
			Envelope currentElement=currentCircle.getMBR();
			
					Integer id=0;
						for(int j=0;j<gridNumberVertical;j++)
						{
							for(int i=0;i<gridNumberHorizontal;i++)
							{
							Envelope currentGrid=new Envelope(gridHorizontalBorder[i],gridHorizontalBorder[i+1],gridVerticalBorder[j],gridVerticalBorder[j+1]);

							if(currentGrid.intersects(currentElement)||currentGrid.contains(currentElement)||currentElement.contains(currentGrid))
							{
								//id=j*gridNumberHorizontal+i;
								list.add(new Tuple2<Integer,Circle>(id,currentCircle));
							}
							id++;
						}
					}
		}
		
		
		return list;
	}
}
