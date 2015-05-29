package Functions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.Polygon;

public class PartitionAssignGridPolygon implements PairFlatMapFunction<java.util.Iterator<Polygon>,Integer,Polygon>, Serializable{
	//This function is to assign grid index to each rectangle in large dataset.
			int gridNumberHorizontal;
			int gridNumberVertical;
			Double[] gridHorizontalBorder;
			Double[] gridVerticalBorder;

			public PartitionAssignGridPolygon(int gridNumberHorizontal, int gridNumberVertical, Double[] gridHorizontalBorder, Double[] gridVerticalBorder) 
			{
				this.gridNumberHorizontal=gridNumberHorizontal;
				this.gridNumberVertical=gridNumberVertical;
				this.gridHorizontalBorder=gridHorizontalBorder;
				this.gridVerticalBorder=gridVerticalBorder;
			}

			public Iterable<Tuple2<Integer, Polygon>> call(Iterator<Polygon> s) throws Exception 	
			{
				ArrayList<Tuple2<Integer, Polygon>> list=new ArrayList<Tuple2<Integer, Polygon>>();
				
				while(s.hasNext())
				{
							
					Polygon currentElement=s.next();
					Integer id=0;
					for(int j=0;j<gridNumberVertical;j++)
							{
								for(int i=0;i<gridNumberHorizontal;i++)
								{
									Envelope currentGrid=new Envelope(gridHorizontalBorder[i],gridHorizontalBorder[i+1],gridVerticalBorder[j],gridVerticalBorder[j+1]);
									if(currentGrid.intersects(currentElement.getEnvelopeInternal())||currentGrid.contains(currentElement.getEnvelopeInternal())||currentElement.getEnvelopeInternal().contains(currentGrid))
									{
										list.add(new Tuple2(id,currentElement));
									}
									id++;
								}
							}
				}
				
				
				return list;
			}
}
