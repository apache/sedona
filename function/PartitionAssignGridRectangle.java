package Functions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import com.vividsolutions.jts.geom.Envelope;

import scala.Tuple2;
import scala.Tuple4;

public class PartitionAssignGridRectangle implements PairFlatMapFunction<java.util.Iterator<Envelope>,Integer,Envelope>, Serializable
{
	//This function is to assign grid index to each rectangle in large dataset.
		int gridNumberHorizontal;
		int gridNumberVertical;
		Double[] gridHorizontalBorder;
		Double[] gridVerticalBorder;
		Tuple2<Integer,Envelope>[] gridFile;
		public PartitionAssignGridRectangle(int gridNumberHorizontal, int gridNumberVertical, Double[] gridHorizontalBorder, Double[] gridVerticalBorder) 
		{
			this.gridNumberHorizontal=gridNumberHorizontal;
			this.gridNumberVertical=gridNumberVertical;
			this.gridHorizontalBorder=gridHorizontalBorder;
			this.gridVerticalBorder=gridVerticalBorder;
		}
		public Iterable<Tuple2<Integer, Envelope>> call(Iterator<Envelope> s) throws Exception 	
		{
			ArrayList<Tuple2<Integer, Envelope>> list=new ArrayList<Tuple2<Integer, Envelope>>();
			
			while(s.hasNext())
			{
						
				Envelope currentElement=s.next();
				
						Integer id=0;
							for(int j=0;j<gridNumberVertical;j++)
							{
								for(int i=0;i<gridNumberHorizontal;i++)
								{
								Envelope currentGrid=new Envelope(gridHorizontalBorder[i],gridHorizontalBorder[i+1],gridVerticalBorder[j],gridVerticalBorder[j+1]);
								if(currentGrid.intersects(currentElement)||currentGrid.contains(currentElement)||currentElement.contains(currentGrid))
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
