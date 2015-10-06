package org.datasyslab.geospark.spatialRDD;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.quadtree.Quadtree;
import com.vividsolutions.jts.index.strtree.STRtree;


class PointRangeQueryWithSTRtree implements FlatMapFunction<Iterator<STRtree>,Point>,Serializable
{	
	private Envelope queryWindow=null;
	public PointRangeQueryWithSTRtree(Envelope QueryWindow)
	{
		this.queryWindow=QueryWindow;
	}
	public Iterable<Point> call(Iterator<STRtree> strtree) {
		List<Point> points=strtree.next().query(queryWindow);
		return points;
	}
	
}
class PointRangeQueryWithQuadtree implements FlatMapFunction<Iterator<Quadtree>,Point>,Serializable
{	
	private Envelope queryWindow=null;
	public PointRangeQueryWithQuadtree(Envelope QueryWindow)
	{
		this.queryWindow=QueryWindow;
	}
	public Iterable<Point> call(Iterator<Quadtree> quadtree) {

		List<Point> points=quadtree.next().query(queryWindow);
		return points;

	}
	
}

public class PointIndexRDD implements Serializable{
	private JavaRDD<STRtree> pointIndexRDDrtree;
	private JavaRDD<Quadtree> pointIndexRDDquadtree;
	private String tree;

	public PointIndexRDD(JavaSparkContext spark, String InputLocation,Integer Offset,String Splitter,Integer partitions,String Tree)
	{

		JavaRDD<Point> pointRDDWithoutTree=spark.textFile(InputLocation,partitions).map(new PointFormatMapper(Offset,Splitter));
		if(Tree=="rtree"){
		this.pointIndexRDDrtree=pointRDDWithoutTree.mapPartitions(new FlatMapFunction<Iterator<Point>,STRtree>()
				{

					public Iterable<STRtree> call(Iterator<Point> points){
					Iterator<Point> targetIterator=points;
					STRtree strtree= new STRtree();
					ArrayList<STRtree> strtrees=new ArrayList<STRtree>(1);
					while(targetIterator.hasNext())
					{
						Point currentTarget=targetIterator.next();
						strtree.insert(currentTarget.getEnvelopeInternal(), currentTarget);
					}
					strtrees.add(strtree);
					return strtrees;
					}
				});
			this.tree="rtree";
		}
		else 
		{
			this.pointIndexRDDquadtree=pointRDDWithoutTree.mapPartitions(new FlatMapFunction<Iterator<Point>,Quadtree>()
					{

						public Iterable<Quadtree> call(Iterator<Point> points){
						Iterator<Point> targetIterator=points;
						Quadtree quadtree= new Quadtree();
						ArrayList<Quadtree> quadtrees=new ArrayList<Quadtree>(1);
						while(targetIterator.hasNext())
						{
							Point currentTarget=targetIterator.next();
							quadtree.insert(currentTarget.getEnvelopeInternal(), currentTarget);
						}
						quadtrees.add(quadtree);
						return quadtrees;
						}
					});
			this.tree="quadtree";
		}
		
		
	}
	public PointIndexRDD(JavaSparkContext spark, String InputLocation,Integer Offset,String Splitter,String Tree)
	{
		JavaRDD<Point> pointRDDWithoutTree=spark.textFile(InputLocation).map(new PointFormatMapper(Offset,Splitter));
		if(Tree=="rtree"){
		this.pointIndexRDDrtree=pointRDDWithoutTree.mapPartitions(new FlatMapFunction<Iterator<Point>,STRtree>()
				{

					public Iterable<STRtree> call(Iterator<Point> points){
					Iterator<Point> targetIterator=points;
					STRtree strtree= new STRtree();
					ArrayList<STRtree> strtrees=new ArrayList<STRtree>(1);
					while(targetIterator.hasNext())
					{
						Point currentTarget=targetIterator.next();
						strtree.insert(currentTarget.getEnvelopeInternal(), currentTarget);
					}
					strtrees.add(strtree);
					return strtrees;
					}
				});
			this.tree="rtree";
		}
		else 
		{
			this.pointIndexRDDquadtree=pointRDDWithoutTree.mapPartitions(new FlatMapFunction<Iterator<Point>,Quadtree>()
					{

						public Iterable<Quadtree> call(Iterator<Point> points){
						Iterator<Point> targetIterator=points;
						Quadtree quadtree= new Quadtree();
						ArrayList<Quadtree> quadtrees=new ArrayList<Quadtree>(1);
						while(targetIterator.hasNext())
						{
							Point currentTarget=targetIterator.next();
							quadtree.insert(currentTarget.getEnvelopeInternal(), currentTarget);
						}
						quadtrees.add(quadtree);
						return quadtrees;
						}
					});
			this.tree="quadtree";
		}
	}


	public PointRDD SpatialRangeQuery(Envelope envelope)
	{
		if(tree=="rtree"){
		JavaRDD<Point> result=this.pointIndexRDDrtree.mapPartitions(new PointRangeQueryWithSTRtree(envelope));
		return new PointRDD(result);
		}
		else
		{
			JavaRDD<Point> result=this.pointIndexRDDquadtree.mapPartitions(new PointRangeQueryWithQuadtree(envelope));
			return new PointRDD(result);

		}
	}
	public PointRDD SpatialRangeQuery(Polygon polygon)
	{
		if(tree=="rtree"){
		JavaRDD<Point> result=this.pointIndexRDDrtree.mapPartitions(new PointRangeQueryWithSTRtree(polygon.getEnvelopeInternal()));
		return new PointRDD(result);
		}
		else
		{
			JavaRDD<Point> result=this.pointIndexRDDquadtree.mapPartitions(new PointRangeQueryWithQuadtree(polygon.getEnvelopeInternal()));
			return new PointRDD(result);

		}
	}
}
