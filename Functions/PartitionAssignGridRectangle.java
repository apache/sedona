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
/*		public Iterable<Tuple2<Integer, Envelope>> call(Iterator<Envelope> s) throws Exception
		{
			Integer id=0;
			ArrayList<Tuple2<Integer, Envelope>> list=new ArrayList<Tuple2<Integer, Envelope>>();
			Iterator<Envelope> targetIterator=s;
			while(targetIterator.hasNext())
			{
				Envelope currentTarget=targetIterator.next();
				
				for(int i=0;i<gridNumberHorizontal*gridNumberVertical;i++)
				{
					Envelope currentGrid=this.gridFile[i]._2();
					if(currentGrid.intersects(currentTarget)||currentGrid.contains(currentTarget)||currentTarget.contains(currentGrid))
					{
						list.add(new Tuple2<Integer,Envelope>(i,currentTarget));
					}
				}
			}
			return list;
		}
		*/
		public Iterable<Tuple2<Integer, Envelope>> call(Iterator<Envelope> s) throws Exception 	
		{
			//int id=-1;
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
								//Boolean Vertex1Condition=currentElement.getMinX()>=gridHorizontalBorder[i] && currentElement.getMinX()<=gridHorizontalBorder[i+1]&&currentElement.getMaxX()>=gridHorizontalBorder[i] && currentElement.getMaxX()<=gridHorizontalBorder[i+1] ;
								//Boolean Vertex2Condition=currentElement.getMinY()>=gridVerticalBorder[j] && currentElement.getMinY()<=gridVerticalBorder[j+1]&&currentElement.getMaxY()>=gridVerticalBorder[j] && currentElement.getMaxY()<=gridVerticalBorder[j+1];
								
								/*if(Vertex1Condition && Vertex2Condition )
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
									}*/
								if(currentGrid.intersects(currentElement)||currentGrid.contains(currentElement)||currentElement.contains(currentGrid))
								{
									//id=j*gridNumberHorizontal+i;
									list.add(new Tuple2(id,currentElement));
								}
								id++;
							}
						}
			}
			
			
			return list;
		}
}
