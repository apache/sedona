/**
 * FILE: SpatialRDD.java
 * PATH: org.datasyslab.geospark.spatialRDD.SpatialRDD.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialRDD;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.geometryObjects.Circle;
import org.datasyslab.geospark.spatialPartitioning.EqualPartitioning;
import org.datasyslab.geospark.spatialPartitioning.HilbertPartitioning;
import org.datasyslab.geospark.spatialPartitioning.PartitionJudgement;
import org.datasyslab.geospark.spatialPartitioning.RtreePartitioning;
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner;
import org.datasyslab.geospark.spatialPartitioning.VoronoiPartitioning;
import org.datasyslab.geospark.utils.RDDSampleUtils;
import org.datasyslab.geospark.utils.XMaxComparator;
import org.datasyslab.geospark.utils.XMinComparator;
import org.datasyslab.geospark.utils.YMaxComparator;
import org.datasyslab.geospark.utils.YMinComparator;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.index.quadtree.Quadtree;
import com.vividsolutions.jts.index.strtree.STRtree;

import scala.Tuple2;

/**
 * The Class SpatialRDD.
 */
public abstract class SpatialRDD implements Serializable{
	
	/** The Constant logger. */
	final static Logger logger = Logger.getLogger(SpatialRDD.class);
    
    /** The total number of records. */
    public long totalNumberOfRecords=-1;
    
    /** The boundary. */
    public Double[] boundary = new Double[4];
    
    /** The boundary envelope. */
    public Envelope boundaryEnvelope = null;
    
    /** The spatial partitioned RDD. */
    public JavaPairRDD<Integer, Object> spatialPartitionedRDD;
    
    /** The indexed RDD. */
    public JavaPairRDD<Integer, Object> indexedRDD;
    
    /** The indexed raw RDD. */
    public JavaRDD<Object> indexedRawRDD;
    
    /** The raw spatial RDD. */
    public JavaRDD<Object> rawSpatialRDD;

	/** The grids. */
    public List<Envelope> grids;
    
	/**
	 * Spatial partitioning.
	 *
	 * @param gridType the grid type
	 * @return true, if successful
	 * @throws Exception the exception
	 */
	public boolean spatialPartitioning(GridType gridType) throws Exception
	{
        int numPartitions = this.rawSpatialRDD.rdd().partitions().length;;
		if(this.boundaryEnvelope==null)
        {
        	throw new Exception("[AbstractSpatialRDD][spatialPartitioning] SpatialRDD boundary is null. Please call boundary() first.");
        }
        if(this.totalNumberOfRecords==-1)
        {
        	throw new Exception("[AbstractSpatialRDD][spatialPartitioning] SpatialRDD volume is unkown. Please call count() first.");
        }
		//Calculate the number of samples we need to take.
        int sampleNumberOfRecords = RDDSampleUtils.getSampleNumbers(numPartitions, this.totalNumberOfRecords);
        //Take Sample
        ArrayList objectSampleList = new ArrayList(this.rawSpatialRDD.takeSample(false, sampleNumberOfRecords));
        //Sort
        if(gridType == GridType.EQUALGRID)
        {
        	EqualPartitioning EqualPartitioning=new EqualPartitioning(this.boundaryEnvelope,numPartitions);
        	grids=EqualPartitioning.getGrids();
        }
        else if(gridType == GridType.HILBERT)
        {
        	HilbertPartitioning hilbertPartitioning=new HilbertPartitioning(objectSampleList,this.boundaryEnvelope,numPartitions);
        	grids=hilbertPartitioning.getGrids();
        }
        else if(gridType == GridType.RTREE)
        {
        	RtreePartitioning rtreePartitioning=new RtreePartitioning(objectSampleList,this.boundaryEnvelope,numPartitions);
        	grids=rtreePartitioning.getGrids();
        }
        else if(gridType == GridType.VORONOI)
        {
        	VoronoiPartitioning voronoiPartitioning=new VoronoiPartitioning(objectSampleList,this.boundaryEnvelope,numPartitions);
        	grids=voronoiPartitioning.getGrids();
        }
        else
        {
        	throw new Exception("[AbstractSpatialRDD][spatialPartitioning] Unsupported spatial partitioning method.");
        }
        JavaPairRDD<Integer, Object> spatialNumberingRDD = this.rawSpatialRDD.flatMapToPair(
                new PairFlatMapFunction<Object, Integer, Object>() {
                    @Override
                    public Iterator<Tuple2<Integer, Object>> call(Object spatialObject) throws Exception {
                    	return PartitionJudgement.getPartitionID(grids,spatialObject);
                    }
                }
        );
        this.spatialPartitionedRDD = spatialNumberingRDD.partitionBy(new SpatialPartitioner(grids.size()));
        return true;
	}
	
	/**
	 * Spatial partitioning.
	 *
	 * @param otherGrids the other grids
	 * @return true, if successful
	 * @throws Exception the exception
	 */
	public boolean spatialPartitioning(final List<Envelope> otherGrids) throws Exception
	{
        JavaPairRDD<Integer, Object> spatialNumberingRDD = this.rawSpatialRDD.flatMapToPair(
                new PairFlatMapFunction<Object, Integer, Object>() {
                    @Override
                    public Iterator<Tuple2<Integer, Object>> call(Object spatialObject) throws Exception {
                    	return PartitionJudgement.getPartitionID(otherGrids,spatialObject);
                    }
                }
        );
        this.grids = otherGrids;
        this.spatialPartitionedRDD = spatialNumberingRDD.partitionBy(new SpatialPartitioner(grids.size()));
        return true;
	}
	
	/**
	 * Count without duplicates.
	 *
	 * @return the long
	 */
	public long countWithoutDuplicates()
	{

		List collectedResult = this.rawSpatialRDD.collect();
		HashSet resultWithoutDuplicates = new HashSet();
		for(int i=0;i<collectedResult.size();i++)
		{
			resultWithoutDuplicates.add(collectedResult.get(i));
		}
		return resultWithoutDuplicates.size();
	}
	
	/**
	 * Count without duplicates SPRDD.
	 *
	 * @return the long
	 */
	public long countWithoutDuplicatesSPRDD()
	{
		JavaRDD cleanedRDD = this.spatialPartitionedRDD.map(new Function<Tuple2<Integer,Object>,Object>()
		{

			@Override
			public Object call(Tuple2<Integer, Object> spatialObjectWithId) throws Exception {
				return spatialObjectWithId._2();
			}
		});
		List collectedResult = cleanedRDD.collect();
		HashSet resultWithoutDuplicates = new HashSet();
		for(int i=0;i<collectedResult.size();i++)
		{
			resultWithoutDuplicates.add(collectedResult.get(i));
		}
		return resultWithoutDuplicates.size();
	}
	
	/**
	 * Builds the index.
	 *
	 * @param indexType the index type
	 * @param buildIndexOnSpatialPartitionedRDD the build index on spatial partitioned RDD
	 * @throws Exception the exception
	 */
	public void buildIndex(final IndexType indexType,boolean buildIndexOnSpatialPartitionedRDD) throws Exception {
	      if (buildIndexOnSpatialPartitionedRDD==false) {
	    	  //This index is built on top of unpartitioned SRDD
	    	  this.indexedRawRDD =  this.rawSpatialRDD.mapPartitions(new FlatMapFunction<Iterator<Object>,Object>()
	    	  {
	    		  @Override
	        	  public Iterator<Object> call(Iterator<Object> spatialObjects) throws Exception {
	        		  if(indexType == IndexType.RTREE)
	        		  {
		        		  STRtree rt = new STRtree();
		        		  while(spatialObjects.hasNext()){
		        			  Object spatialObject = spatialObjects.next();
		        			  if(spatialObject instanceof Envelope)
		        			  {
		        				  GeometryFactory geometryFactory = new GeometryFactory();
		        				  Envelope castedSpatialObject = (Envelope) spatialObject;
		        				  Geometry item= geometryFactory.toGeometry(castedSpatialObject);
		        				  if(castedSpatialObject.getUserData()!=null)
		        				  {
		        					  item.setUserData(castedSpatialObject.getUserData());
		        				  }
		        				  rt.insert(castedSpatialObject, item);
		        			  }
		        			  else if(spatialObject instanceof Geometry)
		        			  {
		        				  Geometry castedSpatialObject = (Geometry) spatialObject;
		        				  rt.insert(castedSpatialObject.getEnvelopeInternal(), castedSpatialObject);
		        			  }
		        			  else
		        			  {
		        				  throw new Exception("[AbstractSpatialRDD][buildIndex] Unsupported spatial partitioning method.");
		        			  }
		        		  }
		        		  HashSet<Object> result = new HashSet<Object>();
		        		  rt.query(new Envelope(0.0,0.0,0.0,0.0));
		        		  result.add(rt);
		        		  return result.iterator();
	        		  }
	        		  else
	        		  {
	        			  Quadtree rt = new Quadtree();
		        		  while(spatialObjects.hasNext()){
		        			  Object spatialObject = spatialObjects.next();
		        			  if(spatialObject instanceof Envelope)
		        			  {
		        				  GeometryFactory geometryFactory = new GeometryFactory();
		        				  Envelope castedSpatialObject = (Envelope) spatialObject;
		        				  Geometry item= geometryFactory.toGeometry(castedSpatialObject);
		        				  if(castedSpatialObject.getUserData()!=null)
		        				  {
		        					  item.setUserData(castedSpatialObject.getUserData());
		        				  }
		        				  rt.insert(castedSpatialObject, item);
		        			  }
		        			  else if(spatialObject instanceof Geometry)
		        			  {
		        				  Geometry castedSpatialObject = (Geometry) spatialObject;
		        				  rt.insert(castedSpatialObject.getEnvelopeInternal(), castedSpatialObject);
		        			  }
		        			  else
		        			  {
		        				  throw new Exception("[AbstractSpatialRDD][buildIndex] Unsupported spatial partitioning method.");
		        			  }
		        		  }
		        		  HashSet<Object> result = new HashSet<Object>();
		        		  rt.query(new Envelope(0.0,0.0,0.0,0.0));
		        		  result.add(rt);
		        		  return result.iterator();
	        		  }

	        	  }
	          	}); 
	        }
	        else
	        {
	        	if(this.spatialPartitionedRDD==null)
	        	{
  				  throw new Exception("[AbstractSpatialRDD][buildIndex] spatialPartitionedRDD is null. Please do spatial partitioning before build index.");
	        	}
	        	JavaPairRDD<Integer, Iterable<Object>> groupBySpatialPartitionedRDD = this.spatialPartitionedRDD.groupByKey();       
	        	this.indexedRDD = groupBySpatialPartitionedRDD.flatMapValues(new Function<Iterable<Object>, Iterable<Object>>() {
	        		@Override
	        		public Iterable<Object> call(Iterable<Object> spatialObjects) throws Exception {
	        			if(indexType == IndexType.RTREE)
	        			{
		        			STRtree rt = new STRtree();
		        			Iterator iterator=spatialObjects.iterator();
		        			while(iterator.hasNext()){
		        				Object spatialObject = iterator.next();
		        				if(spatialObject instanceof Envelope)
		        				{
									Envelope castedSpatialObject = (Envelope) spatialObject;
									rt.insert(castedSpatialObject, castedSpatialObject);
		        				}
		        				else if(spatialObject instanceof Geometry)
		        				{
									Geometry castedSpatialObject = (Geometry) spatialObject;
									rt.insert(castedSpatialObject.getEnvelopeInternal(), castedSpatialObject);
		        				}
		        				else
		        				{
						        	throw new Exception("[AbstractSpatialRDD][buildIndex] Unsupported spatial partitioning method.");
		        				}
		        			}
		        			HashSet<Object> result = new HashSet<Object>();
		        			rt.query(new Envelope(0.0,0.0,0.0,0.0));
		        			result.add(rt);
		        			return result;
	        			}
	        			else
	        			{
		        			Quadtree rt = new Quadtree();
		        			Iterator iterator=spatialObjects.iterator();
		        			while(iterator.hasNext()){
		        				Object spatialObject = iterator.next();
		        				if(spatialObject instanceof Envelope)
		        				{
									Envelope castedSpatialObject = (Envelope) spatialObject;
									rt.insert(castedSpatialObject, castedSpatialObject);
		        				}
		        				else if(spatialObject instanceof Geometry)
		        				{
									Geometry castedSpatialObject = (Geometry) spatialObject;
									rt.insert(castedSpatialObject.getEnvelopeInternal(), castedSpatialObject);
		        				}
		        				else
		        				{
						        	throw new Exception("[AbstractSpatialRDD][buildIndex] Unsupported spatial partitioning method.");
		        				}
		        			}
		        			HashSet<Object> result = new HashSet<Object>();
		        			rt.query(new Envelope(0.0,0.0,0.0,0.0));
		        			result.add(rt);
		        			return result;
	        			}
	        		}
	        	}
	        	);
	        }
	}
	
    /**
     * Boundary.
     *
     * @return the envelope
     */
    public Envelope boundary() {
        
        try
        {
        	Object minXEnvelope = this.rawSpatialRDD.min(new XMinComparator());
        	Object minYEnvelope = this.rawSpatialRDD.min(new YMinComparator());
        	Object maxXEnvelope = this.rawSpatialRDD.max(new XMaxComparator());
        	Object maxYEnvelope = this.rawSpatialRDD.max(new YMaxComparator());
        	
        	this.boundary[0] = ((Geometry)minXEnvelope).getEnvelopeInternal().getMinX();
            this.boundary[1] = ((Geometry)minYEnvelope).getEnvelopeInternal().getMinY();
            this.boundary[2] = ((Geometry)maxXEnvelope).getEnvelopeInternal().getMaxX();
            this.boundary[3] = ((Geometry)maxYEnvelope).getEnvelopeInternal().getMaxY();
        }
        catch(ClassCastException castError)
        {
        	if(castError.getMessage().contains("Circle"))
        	{
            	Object minXEnvelope = this.rawSpatialRDD.min(new XMinComparator());
            	Object minYEnvelope = this.rawSpatialRDD.min(new YMinComparator());
            	Object maxXEnvelope = this.rawSpatialRDD.max(new XMaxComparator());
            	Object maxYEnvelope = this.rawSpatialRDD.max(new YMaxComparator());
            	
            	this.boundary[0] = ((Circle)minXEnvelope).getMBR().getMinX();
                this.boundary[1] = ((Circle)minYEnvelope).getMBR().getMinY();
                this.boundary[2] = ((Circle)maxXEnvelope).getMBR().getMaxX();
                this.boundary[3] = ((Circle)maxYEnvelope).getMBR().getMaxY();
        	}
        	else if(castError.getMessage().contains("Envelope"))
        	{
            	Object minXEnvelope = this.rawSpatialRDD.min(new XMinComparator());
            	Object minYEnvelope = this.rawSpatialRDD.min(new YMinComparator());
            	Object maxXEnvelope = this.rawSpatialRDD.max(new XMaxComparator());
            	Object maxYEnvelope = this.rawSpatialRDD.max(new YMaxComparator());
            	
            	this.boundary[0] = ((Envelope)minXEnvelope).getMinX();
                this.boundary[1] = ((Envelope)minYEnvelope).getMinY();
                this.boundary[2] = ((Envelope)maxXEnvelope).getMaxX();
                this.boundary[3] = ((Envelope)maxYEnvelope).getMaxY();
        	}
        }
        this.boundaryEnvelope =  new Envelope(boundary[0],boundary[2],boundary[1],boundary[3]);
        return this.boundaryEnvelope;
    }
	
	/**
	 * Gets the raw spatial RDD.
	 *
	 * @return the raw spatial RDD
	 */
	public JavaRDD<Object> getRawSpatialRDD() {
		return rawSpatialRDD;
	}

	/**
	 * Sets the raw spatial RDD.
	 *
	 * @param rawSpatialRDD the new raw spatial RDD
	 */
	public void setRawSpatialRDD(JavaRDD<Object> rawSpatialRDD) {
		this.rawSpatialRDD = rawSpatialRDD;
	}
	
	/**
	 * Analyze.
	 *
	 * @param newLevel the new level
	 * @return true, if successful
	 */
	public boolean analyze(StorageLevel newLevel)
	{
		this.rawSpatialRDD.persist(newLevel);
		this.boundary();
        this.totalNumberOfRecords = this.rawSpatialRDD.count();
        return true;
	}
	
	/**
	 * Analyze.
	 *
	 * @return true, if successful
	 */
	public boolean analyze()
	{
		this.boundary();
        this.totalNumberOfRecords = this.rawSpatialRDD.count();
        return true;
	}
}
