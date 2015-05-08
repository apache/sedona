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

		public PartitionAssignGridRectangle(int gridNumberHorizontal, int gridNumberVertical, Double[] gridHorizontalBorder, Double[] gridVerticalBorder) 
		{
			this.gridNumberHorizontal=gridNumberHorizontal;
			this.gridNumberVertical=gridNumberVertical;
			this.gridHorizontalBorder=gridHorizontalBorder;
			this.gridVerticalBorder=gridVerticalBorder;
		}

		public Iterable<Tuple2<Integer, Envelope>> call(Iterator<Envelope> s) throws Exception 	
		{
			int id=-1;
			ArrayList<Tuple2<Integer, Envelope>> list=new ArrayList<Tuple2<Integer, Envelope>>();
			
			while(s.hasNext())
			{
						
				Envelope currentElement=s.next();
						for(int i=0;i<gridNumberHorizontal;i++)
						{
							for(int j=0;j<gridNumberVertical;j++)
							{
								Boolean Vertex1Condition=currentElement.getMinX()>=gridHorizontalBorder[i] && currentElement.getMinX()<=gridHorizontalBorder[i+1]&&currentElement.getMaxX()>=gridHorizontalBorder[i] && currentElement.getMaxX()<=gridHorizontalBorder[i+1] ;
								Boolean Vertex2Condition=currentElement.getMinY()>=gridVerticalBorder[j] && currentElement.getMinY()<=gridVerticalBorder[j+1]&&currentElement.getMaxY()>=gridVerticalBorder[j] && currentElement.getMaxY()<=gridVerticalBorder[j+1];
								
								if(Vertex1Condition && Vertex2Condition )
								{
									//Fully contain
									id=i*gridNumberHorizontal+j;
									list.add(new Tuple2(id,currentElement));
								}
								else if(!Vertex1Condition && !Vertex2Condition)
									{
										//Fully disjoint
										continue;
									}
									else
									{
										//Overlap
										id=i*gridNumberHorizontal+j;
										list.add(new Tuple2(id,currentElement));
									}
							}
						}
			}
			
			
			return list;
		}
}
