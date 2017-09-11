/**
 * FILE: SpatialRDD.java
 * PATH: org.datasyslab.geospark.spatialRDD.SpatialRDD.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialRDD;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.SpatialIndex;
import com.vividsolutions.jts.index.quadtree.Quadtree;
import com.vividsolutions.jts.index.strtree.STRtree;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.spatialPartitioning.EqualPartitioning;
import org.datasyslab.geospark.spatialPartitioning.HilbertPartitioning;
import org.datasyslab.geospark.spatialPartitioning.PartitionJudgement;
import org.datasyslab.geospark.spatialPartitioning.QuadtreePartitioning;
import org.datasyslab.geospark.spatialPartitioning.RtreePartitioning;
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner;
import org.datasyslab.geospark.spatialPartitioning.VoronoiPartitioning;
import org.datasyslab.geospark.spatialPartitioning.quadtree.StandardQuadTree;
import org.datasyslab.geospark.utils.RDDSampleUtils;
import org.datasyslab.geospark.utils.XMaxComparator;
import org.datasyslab.geospark.utils.XMinComparator;
import org.datasyslab.geospark.utils.YMaxComparator;
import org.datasyslab.geospark.utils.YMinComparator;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.wololo.geojson.Feature;
import org.wololo.jts2geojson.GeoJSONWriter;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

// TODO: Auto-generated Javadoc
/**
 * The Class SpatialRDD.
 */
public class SpatialRDD<T extends Geometry> implements Serializable{

	/** The Constant logger. */
	final static Logger logger = Logger.getLogger(SpatialRDD.class);

    /** The total number of records. */
    public long approximateTotalCount=-1;

    /** The boundary envelope. */
    public Envelope boundaryEnvelope = null;

    /** The spatial partitioned RDD. */
    public JavaRDD<T> spatialPartitionedRDD;

    /** The indexed RDD. */
    public JavaRDD<SpatialIndex> indexedRDD;

    /** The indexed raw RDD. */
    public JavaRDD<SpatialIndex> indexedRawRDD;

    /** The raw spatial RDD. */
    public JavaRDD<T> rawSpatialRDD;

	/** The grids. */
    public List<Envelope> grids;

    public StandardQuadTree partitionTree;

	/** The CR stransformation. */
	protected boolean CRStransformation=false;;

	/** The source epsg code. */
	protected String sourceEpsgCode="";

	/** The target epgsg code. */
	protected String targetEpgsgCode="";

	/**
	 * CRS transform.
	 *
	 * @param sourceEpsgCRSCode the source epsg CRS code
	 * @param targetEpsgCRSCode the target epsg CRS code
	 * @return true, if successful
	 */
	public boolean CRSTransform(String sourceEpsgCRSCode, String targetEpsgCRSCode)
	{
		try {
    	CoordinateReferenceSystem sourceCRS = CRS.decode(sourceEpsgCRSCode);
		CoordinateReferenceSystem targetCRS = CRS.decode(targetEpsgCRSCode);
		final MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS, false);
		this.CRStransformation=true;
		this.sourceEpsgCode=sourceEpsgCRSCode;
		this.targetEpgsgCode=targetEpsgCRSCode;
		this.rawSpatialRDD = this.rawSpatialRDD.map(new Function<T,T>()
		{
			@Override
			public T call(T originalObject) throws Exception {
				return (T) JTS.transform(originalObject,transform);
			}
		});
		return true;
		} catch (FactoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}

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
        	throw new Exception("[AbstractSpatialRDD][spatialPartitioning] SpatialRDD boundary is null. Please call analyze() first.");
        }
        if(this.approximateTotalCount==-1)
        {
        	throw new Exception("[AbstractSpatialRDD][spatialPartitioning] SpatialRDD total count is unkown. Please call analyze() first.");
        }
		//Calculate the number of samples we need to take.
        int sampleNumberOfRecords = RDDSampleUtils.getSampleNumbers(numPartitions, this.approximateTotalCount);
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
		else if (gridType == GridType.QUADTREE) {
			QuadtreePartitioning quadtreePartitioning = new QuadtreePartitioning(objectSampleList, this.boundaryEnvelope, numPartitions);
			partitionTree = quadtreePartitioning.getPartitionTree();
		}
        else
        {
        	throw new Exception("[AbstractSpatialRDD][spatialPartitioning] Unsupported spatial partitioning method.");
        }


		if(gridType == GridType.QUADTREE)
		{
			this.spatialPartitionedRDD = partitionUsingQuadTree(partitionTree);
		}
		else
		{
			this.spatialPartitionedRDD = partitionUsingGrids(grids);
		}

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
		this.spatialPartitionedRDD = partitionUsingGrids(otherGrids);
		this.grids = otherGrids;
		return true;
	}

	public boolean spatialPartitioning(final StandardQuadTree partitionTree) throws Exception {
		this.spatialPartitionedRDD = partitionUsingQuadTree(partitionTree);
		this.partitionTree = partitionTree;
		return true;
	}

	private static final class GetTuple2Value<T>
			implements FlatMapFunction<Iterator<Tuple2<Integer, T>>, T> {

		@Override
		public Iterator<T> call(final Iterator<Tuple2<Integer, T>> iterator) throws Exception {
			return new Iterator<T>() {
				@Override
				public boolean hasNext() {
					return iterator.hasNext();
				}

				@Override
				public T next() {
					return iterator.next()._2();
				}

				@Override
				public void remove() {
					throw new UnsupportedOperationException();
				}
			};
		}
	}

	private JavaRDD<T> partitionUsingQuadTree(final StandardQuadTree<T> quadTree) {
		final SpatialPartitioner partitioner = new SpatialPartitioner(quadTree.getTotalNumLeafNode());
		return this.rawSpatialRDD.flatMapToPair(
				new PairFlatMapFunction<T, Integer, T>() {
					@Override
					public Iterator<Tuple2<Integer, T>> call(T spatialObject) throws Exception {
						return PartitionJudgement.getPartitionID(quadTree, spatialObject);
					}
				}
		).partitionBy(partitioner)
				.mapPartitions(new GetTuple2Value<T>(),true);
	}

	private JavaRDD<T> partitionUsingGrids(final List<Envelope> grids) {
		final SpatialPartitioner partitioner = new SpatialPartitioner(grids.size());
		return this.rawSpatialRDD.flatMapToPair(
				new PairFlatMapFunction<T, Integer, T>() {
					@Override
					public Iterator<Tuple2<Integer, T>> call(T spatialObject) throws Exception {
						return PartitionJudgement.getPartitionID(grids, spatialObject);
					}
				}
		).partitionBy(partitioner)
				.mapPartitions(new GetTuple2Value(), true);
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
		JavaRDD cleanedRDD = this.spatialPartitionedRDD;
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
	    	  this.indexedRawRDD =  this.rawSpatialRDD.mapPartitions(new FlatMapFunction<Iterator<T>, SpatialIndex>()
	    	  {
	    		  @Override
	        	  public Iterator<SpatialIndex> call(Iterator<T> spatialObjects) throws Exception {
	        		  if(indexType == IndexType.RTREE)
	        		  {
		        		  STRtree rt = new STRtree();
		        		  while(spatialObjects.hasNext()){
		        			  Geometry spatialObject = spatialObjects.next();
		        			  rt.insert(spatialObject.getEnvelopeInternal(), spatialObject);
		        		  }
		        		  Set<SpatialIndex> result = new HashSet<>();
		        		  rt.query(new Envelope(0.0,0.0,0.0,0.0));
		        		  result.add(rt);
		        		  return result.iterator();
	        		  }
	        		  else
	        		  {
	        			  Quadtree rt = new Quadtree();
		        		  while(spatialObjects.hasNext()){
		        			  Geometry spatialObject = spatialObjects.next();
		        			  rt.insert(spatialObject.getEnvelopeInternal(), spatialObject);
		        		  }
						  Set<SpatialIndex> result = new HashSet<>();
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
				this.indexedRDD = this.spatialPartitionedRDD.mapPartitions(new FlatMapFunction<Iterator<T>, SpatialIndex>() {
					@Override
					public Iterator<SpatialIndex> call(Iterator<T> objectIterator) throws Exception {
						if (indexType == IndexType.RTREE) {
							STRtree rt = new STRtree();
							while (objectIterator.hasNext()) {
								Geometry spatialObject = objectIterator.next();
								rt.insert(spatialObject.getEnvelopeInternal(), spatialObject);
							}
							Set<SpatialIndex> result = new HashSet();
							rt.query(new Envelope(0.0, 0.0, 0.0, 0.0));
							result.add(rt);
							return result.iterator();
						} else {
							Quadtree rt = new Quadtree();
							while (objectIterator.hasNext()) {
								Geometry spatialObject = objectIterator.next();
								rt.insert(spatialObject.getEnvelopeInternal(), spatialObject);
							}
							Set<SpatialIndex> result = new HashSet();
							rt.query(new Envelope(0.0, 0.0, 0.0, 0.0));
							result.add(rt);
							return result.iterator();
						}
					}
				});
	        }
	}

    /**
     * Boundary.
     *
     * @return the envelope
     */
    public Envelope boundary() {
    	Object minXEnvelope = this.rawSpatialRDD.min(new XMinComparator());
    	Object minYEnvelope = this.rawSpatialRDD.min(new YMinComparator());
    	Object maxXEnvelope = this.rawSpatialRDD.max(new XMaxComparator());
    	Object maxYEnvelope = this.rawSpatialRDD.max(new YMaxComparator());
    	Double[] boundary = new Double[4];
    	boundary[0] = ((Geometry) minXEnvelope).getEnvelopeInternal().getMinX();
    	boundary[1] = ((Geometry) minYEnvelope).getEnvelopeInternal().getMinY();
    	boundary[2] = ((Geometry) maxXEnvelope).getEnvelopeInternal().getMaxX();
    	boundary[3] = ((Geometry) maxYEnvelope).getEnvelopeInternal().getMaxY();
        this.boundaryEnvelope =  new Envelope(boundary[0],boundary[2],boundary[1],boundary[3]);
        return this.boundaryEnvelope;
    }

	/**
	 * Gets the raw spatial RDD.
	 *
	 * @return the raw spatial RDD
	 */
	public JavaRDD<T> getRawSpatialRDD() {
		return rawSpatialRDD;
	}

	/**
	 * Sets the raw spatial RDD.
	 *
	 * @param rawSpatialRDD the new raw spatial RDD
	 */
	public void setRawSpatialRDD(JavaRDD<T> rawSpatialRDD) {
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
        this.approximateTotalCount = this.rawSpatialRDD.count();
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
        this.approximateTotalCount = this.rawSpatialRDD.count();
        return true;
	}

	public boolean analyze(Envelope datasetBoundary, Integer approximateTotalCount)
	{
		this.boundaryEnvelope = datasetBoundary;
		this.approximateTotalCount = this.rawSpatialRDD.count();
		return true;
	}

    /**
     * Save as geo JSON.
     *
     * @param outputLocation the output location
     */
    public void saveAsGeoJSON(String outputLocation) {
        this.rawSpatialRDD.mapPartitions(new FlatMapFunction<Iterator<T>, String>() {
            @Override
            public Iterator<String> call(Iterator<T> iterator) throws Exception {
                ArrayList<String> result = new ArrayList();
                GeoJSONWriter writer = new GeoJSONWriter();
                while (iterator.hasNext()) {
                	Geometry spatialObject = (Geometry)iterator.next();
                    Feature jsonFeature;
                    if(spatialObject.getUserData()!=null)
                    {
                        Map<String,Object> userData = new HashMap<String,Object>();
                        userData.put("UserData", spatialObject.getUserData());
                    	jsonFeature = new Feature(writer.write(spatialObject),userData);
                    }
                    else
                    {
                    	jsonFeature = new Feature(writer.write(spatialObject),null);
                    }
                    String jsonstring = jsonFeature.toString();
                    result.add(jsonstring);
                }
                return result.iterator();
            }
        }).saveAsTextFile(outputLocation);
    }

    /**
     * Minimum bounding rectangle.
     *
     * @return the rectangle RDD
     */
    @Deprecated
    public RectangleRDD MinimumBoundingRectangle() {
        JavaRDD<Polygon> rectangleRDD = this.rawSpatialRDD.map(new Function<T, Polygon>() {
            public Polygon call(T spatialObject) {
        		Double x1,x2,y1,y2;
                LinearRing linear;
                Coordinate[] coordinates = new Coordinate[5];
                GeometryFactory fact = new GeometryFactory();
				final Envelope envelope = spatialObject.getEnvelopeInternal();
				x1 = envelope.getMinX();
				x2 = envelope.getMaxX();
				y1 = envelope.getMinY();
				y2 = envelope.getMaxY();
		        coordinates[0]=new Coordinate(x1,y1);
		        coordinates[1]=new Coordinate(x1,y2);
		        coordinates[2]=new Coordinate(x2,y2);
		        coordinates[3]=new Coordinate(x2,y1);
		        coordinates[4]=coordinates[0];
                linear = fact.createLinearRing(coordinates);
                Polygon polygonObject = new Polygon(linear, null, fact);
                return polygonObject;
            }
        });
        return new RectangleRDD(rectangleRDD);
    }

	/**
	 * Gets the CR stransformation.
	 *
	 * @return the CR stransformation
	 */
	public boolean getCRStransformation() {
		return CRStransformation;
	}

	/**
	 * Gets the source epsg code.
	 *
	 * @return the source epsg code
	 */
	public String getSourceEpsgCode() {
		return sourceEpsgCode;
	}

	/**
	 * Gets the target epgsg code.
	 *
	 * @return the target epgsg code
	 */
	public String getTargetEpgsgCode() {
		return targetEpgsgCode;
	}


}
