/**
 * FILE: VisualizationOperator.java
 * PATH: org.datasyslab.babylon.core.VisualizationOperator.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.core;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import org.datasyslab.babylon.utils.RasterizationUtils;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

import scala.Tuple2;


class PixelCountComparator implements Comparator<Tuple2<Integer, Long>>, Serializable
{

	@Override
	public int compare(Tuple2<Integer, Long> spatialObject1, Tuple2<Integer, Long> spatialObject2) {
		if(spatialObject1._2>spatialObject2._2)
		{
			return 1;
		}
		else if(spatialObject1._2<spatialObject2._2)
		{
			return -1;
		}
		else return 0;
	}
}
class PixelSerialIdComparator implements Comparator<Tuple2<Integer, Long>>
{

	@Override
	public int compare(Tuple2<Integer, Long> spatialObject1, Tuple2<Integer, Long> spatialObject2) {
		if(spatialObject1._1>spatialObject2._1)
		{
			return 1;
		}
		else if(spatialObject1._1<spatialObject2._1)
		{
			return -1;
		}
		else return 0;
	}
}

/**
 * The Class VisualizationOperator.
 */
public abstract class VisualizationOperator implements Serializable{
	
	/** The cumulative count. */
	protected boolean cumulativeCount=false; // Scatter plot doesn't use cumulative count.
	
	/** The reverse spatial coordinate. */
	protected boolean reverseSpatialCoordinate; // Switch the x and y when draw the final image
	
	/** The count matrix. */
	protected List<Tuple2<Integer,Long>> countMatrix;
	
	/** The distributed count matrix. */
	protected JavaPairRDD<Integer, Long> distributedCountMatrix;
	
	/** The distributed color matrix. */
	protected JavaPairRDD<Integer, Color> distributedColorMatrix;
	
	/** The resolution X. */
	protected int resolutionX;
	
	/** The resolution Y. */
	protected int resolutionY;
	
	/** The dataset boundary. */
	protected Envelope datasetBoundary;
	
	/** The red. */
	protected int red=255;
	
	/** The green. */
	protected int green=255;
	
	/** The blue. */
	protected int blue=255;
	
	/** The color alpha. */
	protected int colorAlpha=0;
	
	/** The control color channel. */
	protected Color controlColorChannel=Color.green;
	
	/** The use inverse ratio for control color channel. */
	protected boolean useInverseRatioForControlColorChannel = true;
	
	/** The pixel image. */
	public BufferedImage pixelImage=null;
	
	/** The distributed pixel image. */
	public JavaPairRDD<Integer,ImageSerializableWrapper> distributedPixelImage=null;

	/** The Photo filter convolution matrix. */
	/*
	 * Parameters controls Photo Filter
	 */
	protected Double[][] PhotoFilterConvolutionMatrix=null;
	
	/** The photo filter radius. */
	protected int photoFilterRadius;
	
	/** The partition X. */
	/*
	 * Parameter controls spatial partitioning
	 */
	protected int partitionX;
	
	/** The partition Y. */
	protected int partitionY;
	
	/** The partition interval X. */
	protected int partitionIntervalX;
	
	/** The partition interval Y. */
	protected int partitionIntervalY;
	
	/** The has been spatial partitioned. */
	private boolean hasBeenSpatialPartitioned=false;
	
	/** The parallel photo filter. */
	/*
	 * Parameter tells whether do photo filter in parallel and do rendering in parallel
	 */
	protected boolean parallelPhotoFilter = false;
	
	/** The parallel render image. */
	protected boolean parallelRenderImage = false;
		
	/** The spark context. */
	/*
	 * Parameter for the overall system
	 */
	JavaSparkContext sparkContext;
	
	/** The Constant logger. */
	final static Logger logger = Logger.getLogger(VisualizationOperator.class);

	/**
	 * Instantiates a new visualization operator.
	 *
	 * @param resolutionX the resolution X
	 * @param resolutionY the resolution Y
	 * @param datasetBoundary the dataset boundary
	 * @param cumulativeCount the cumulative count
	 * @param reverseSpatialCoordinate the reverse spatial coordinate
	 */
	public VisualizationOperator(int resolutionX, int resolutionY,Envelope datasetBoundary, boolean cumulativeCount, boolean reverseSpatialCoordinate)
	{
		logger.debug("[VisualizationOperator][Constructor][Start]");
		this.resolutionX = resolutionX;
		this.resolutionY = resolutionY;
		this.datasetBoundary = datasetBoundary;
		this.countMatrix = new ArrayList<Tuple2<Integer,Long>>();
		this.cumulativeCount = cumulativeCount;
		this.reverseSpatialCoordinate = reverseSpatialCoordinate;
		int serialId = 0;
		for(int j=0;j<resolutionY;j++)
		{
			for(int i=0;i<resolutionX;i++)
			{
				countMatrix.add(new Tuple2<Integer,Long>(serialId,(long) 0));
				serialId++;
			}
		}
		logger.debug("[VisualizationOperator][Constructor][Stop]");
	}
	

	/**
	 * Instantiates a new visualization operator.
	 *
	 * @param resolutionX the resolution X
	 * @param resolutionY the resolution Y
	 * @param datasetBoundary the dataset boundary
	 * @param cumulativeCount the cumulative count
	 * @param reverseSpatialCoordinate the reverse spatial coordinate
	 * @param partitionX the partition X
	 * @param partitionY the partition Y
	 * @param parallelPhotoFilter the parallel photo filter
	 * @param parallelRenderImage the parallel render image
	 */
	public VisualizationOperator(int resolutionX, int resolutionY,Envelope datasetBoundary, boolean cumulativeCount, boolean reverseSpatialCoordinate,
			int partitionX, int partitionY, boolean parallelPhotoFilter, boolean parallelRenderImage)
	{
		logger.debug("[VisualizationOperator][Constructor][Start]");
		this.resolutionX = resolutionX;
		this.resolutionY = resolutionY;
		this.datasetBoundary = datasetBoundary;
		this.countMatrix = new ArrayList<Tuple2<Integer,Long>>();
		this.cumulativeCount = cumulativeCount;
		this.reverseSpatialCoordinate = reverseSpatialCoordinate;
		int serialId = 0;
		for(int j=0;j<resolutionY;j++)
		{
			for(int i=0;i<resolutionX;i++)
			{
				countMatrix.add(new Tuple2<Integer,Long>(serialId,(long) 0));
				serialId++;
			}
		}
		this.partitionX = partitionX;
		this.partitionY = partitionY;
		this.partitionIntervalX = this.resolutionX/this.partitionX;
		this.partitionIntervalY = this.resolutionY/this.partitionY;
		this.parallelPhotoFilter = parallelPhotoFilter;
		this.parallelRenderImage = parallelRenderImage;
		logger.debug("[VisualizationOperator][Constructor][Stop]");
	}
	
	/**
	 * Inits the photo filter weight matrix.
	 *
	 * @param photoFilter the photo filter
	 * @return true, if successful
	 */
	protected boolean InitPhotoFilterWeightMatrix(PhotoFilter photoFilter)
	{
		this.photoFilterRadius = photoFilter.getFilterRadius();
		this.PhotoFilterConvolutionMatrix =  photoFilter.getConvolutionMatrix();
		return true;
	}
	
	/**
	 * Spatial partitioning.
	 *
	 * @return the java pair RDD
	 * @throws Exception the exception
	 */
	private JavaPairRDD<Integer,Long> spatialPartitioning() throws Exception
	{
		this.distributedCountMatrix = this.distributedCountMatrix.partitionBy(new VisualizationPartitioner(this.resolutionX,this.resolutionY,this.partitionX,this.partitionY));
		return this.distributedCountMatrix;
	}
	
	
	/**
	 * Apply photo filter.
	 *
	 * @param sparkContext the spark context
	 * @return the java pair RDD
	 * @throws Exception the exception
	 */
	protected JavaPairRDD<Integer, Long> ApplyPhotoFilter(JavaSparkContext sparkContext) throws Exception
	{
		logger.debug("[VisualizationOperator][ApplyPhotoFilter][Start]");
		if(this.parallelPhotoFilter)
		{
			if(this.hasBeenSpatialPartitioned==false)
			{
				this.spatialPartitioning();
				this.hasBeenSpatialPartitioned = true;
			}
			this.distributedCountMatrix = this.distributedCountMatrix.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Integer,Long>>,Integer,Long>()
			{

				@Override
				public Iterator<Tuple2<Integer, Long>> call(Iterator<Tuple2<Integer, Long>> currentPartition) throws Exception  {
					//This function will iterate all tuples within this partition twice. Complexity 2N. 
					//First round, iterate all tuples to build a hash map. This hash map guarantees constant <key, value> access time.
					// Another way is to do a sort on the data and then achieve constant access time in the second round. Complexity is N*log(N).
					HashMap<Integer,Long> pixelCountHashMap = new HashMap<Integer,Long>();
					List<Tuple2<Integer,Long>> oldPixelCounts = new ArrayList<Tuple2<Integer,Long>>();
					List<Tuple2<Integer,Long>> newPixelCounts = new ArrayList<Tuple2<Integer,Long>>();
					while(currentPartition.hasNext())
					{
						Tuple2<Integer, Long> currentPixelCount = currentPartition.next();
						pixelCountHashMap.put(currentPixelCount._1, currentPixelCount._2);
						oldPixelCounts.add(currentPixelCount);
					}
					//Second round, update the original count by applying photo filter
					for(int i=0;i<oldPixelCounts.size();i++)
					{
						Tuple2<Integer,Integer> centerPixelCoordinate = RasterizationUtils.Decode1DTo2DId(resolutionX, resolutionY, oldPixelCounts.get(i)._1);
						Long pixelCount = new Long(0);
						int neighborCount=0;
						for (int x = -photoFilterRadius; x <= photoFilterRadius; x++) {
							for (int y = -photoFilterRadius; y <= photoFilterRadius; y++) {
								int neighborPixelX = centerPixelCoordinate._1+x;
								int neighborPixelY = centerPixelCoordinate._2+y;
								if(neighborPixelX<resolutionX&&neighborPixelX>=0&&neighborPixelY<resolutionY&&neighborPixelY>=0)
								{
									Long neighborPixelCount =pixelCountHashMap.get(RasterizationUtils.Encode2DTo1DId(resolutionX, resolutionY, neighborPixelX, neighborPixelY));
									if(neighborPixelCount==null)
									{
										//This neighbor is not in this partition. Skip. Each center pixel should find at least (radius+1)*(radius+1) neighbors in its own partition.
										continue;
									}
									neighborCount++;
									pixelCount+=Math.round(neighborPixelCount*PhotoFilterConvolutionMatrix[x + photoFilterRadius][y + photoFilterRadius]);
								}
							}
						}
						Tuple2<Integer,Long> newCenterPixel = new Tuple2<Integer,Long>(oldPixelCounts.get(i)._1,pixelCount);
						newPixelCounts.add(newCenterPixel);
						if(neighborCount<(photoFilterRadius+1)*(photoFilterRadius+1))
						{
							throw new Exception("[VisualizationOperator][ApplyPhotoFilter] The pixel gets too few neighbors:" + neighborCount +"; Expected "+(photoFilterRadius+1)*(photoFilterRadius+1));
						}
					}
					return newPixelCounts.iterator();
				}
			});
		}
		else
		{
			List<Tuple2<Integer,Long>> collectedCountMatrix = this.distributedCountMatrix.collect();
			List<Tuple2<Integer,Long>> originalCountMatrix = new ArrayList<Tuple2<Integer,Long>>(collectedCountMatrix);
			Collections.sort(originalCountMatrix, new PixelSerialIdComparator());
			final Broadcast<List<Tuple2<Integer,Long>>> broadcastCountMatrix = sparkContext.broadcast(originalCountMatrix);
			this.distributedCountMatrix = this.distributedCountMatrix.mapToPair(new PairFunction<Tuple2<Integer,Long>,Integer,Long>()
			{
				@Override
				public Tuple2<Integer, Long> call(Tuple2<Integer, Long> pixel) throws Exception {
					Tuple2<Integer,Integer> centerPixelCoordinate = RasterizationUtils.Decode1DTo2DId(resolutionX, resolutionY, pixel._1);
					Long pixelCount = new Long(0);
					for (int x = -photoFilterRadius; x <= photoFilterRadius; x++) {
						for (int y = -photoFilterRadius; y <= photoFilterRadius; y++) {
							int neighborPixelX = centerPixelCoordinate._1+x;
							int neighborPixelY = centerPixelCoordinate._2+y;
							if(neighborPixelX<resolutionX&&neighborPixelX>=0&&neighborPixelY<resolutionY&&neighborPixelY>=0)
							{
								Long neighborPixelCount = broadcastCountMatrix.getValue().get(RasterizationUtils.Encode2DTo1DId(resolutionX, resolutionY, neighborPixelX, neighborPixelY))._2;
								pixelCount+=Math.round(neighborPixelCount*PhotoFilterConvolutionMatrix[x + photoFilterRadius][y + photoFilterRadius]);
							}
						}
					}
					return new Tuple2<Integer,Long>(pixel._1,pixelCount);
				}				
			});
		}
		logger.debug("[VisualizationOperator][ApplyPhotoFilter][Stop]");
		return this.distributedCountMatrix;
	}
	
	
	/**
	 * Generate color matrix.
	 *
	 * @return the java pair RDD
	 */
	protected JavaPairRDD<Integer, Color> GenerateColorMatrix()
	{
		logger.debug("[VisualizationOperator][GenerateColorMatrix][Start]");
		final long maxWeight = this.distributedCountMatrix.max(new PixelCountComparator())._2;
		final long minWeight = 0;
		this.distributedColorMatrix = this.distributedCountMatrix.mapToPair(new PairFunction<Tuple2<Integer,Long>,Integer,Color>()
		{

			@Override
			public Tuple2<Integer, Color> call(Tuple2<Integer, Long> pixelCount) throws Exception {
				Long normalizedPixelCount = (pixelCount._2-minWeight)*255/(maxWeight-minWeight);
				Color pixelColor = EncodeToColor(normalizedPixelCount.intValue());
				return new Tuple2<Integer,Color>(pixelCount._1,pixelColor);
			}
		});
		logger.debug("[VisualizationOperator][GenerateColorMatrix][Stop]");
		return this.distributedColorMatrix;
	}
	
	/**
	 * Render image.
	 *
	 * @return true, if successful
	 * @throws Exception the exception
	 */
	protected boolean RenderImage() throws Exception
	{
		logger.debug("[VisualizationOperator][RenderImage][Start]");
		if(this.parallelRenderImage)
		{
			if(this.hasBeenSpatialPartitioned==false)
			{
				this.spatialPartitioning();
				this.hasBeenSpatialPartitioned = true;
			}
			this.distributedPixelImage = this.distributedColorMatrix.mapPartitionsToPair(
					new PairFlatMapFunction<Iterator<Tuple2<Integer,Color>>,Integer,ImageSerializableWrapper>()
			{

				@Override
				public Iterator<Tuple2<Integer, ImageSerializableWrapper>> call(Iterator<Tuple2<Integer, Color>> currentPartition)
						throws Exception {
					BufferedImage imagePartition = new BufferedImage(partitionIntervalX,partitionIntervalY,BufferedImage.TYPE_INT_ARGB);
					Tuple2<Integer,Color> pixelColor=null;
					Tuple2<Integer,Integer> pixelCoordinate = null;
					while(currentPartition.hasNext())
					{
						//Render color in this image partition pixel-wise.
						pixelColor = currentPartition.next();
						pixelCoordinate =  RasterizationUtils.Decode1DTo2DId(resolutionX, resolutionY, pixelColor._1);
						imagePartition.setRGB(pixelCoordinate._1%partitionIntervalX, (partitionIntervalY -1)-pixelCoordinate._2%partitionIntervalY, pixelColor._2.getRGB());
					}
					int partitionCoordinateX = pixelCoordinate._1/partitionIntervalX;
					int partitionCoordinateY = pixelCoordinate._2/partitionIntervalY;
					List<Tuple2<Integer, ImageSerializableWrapper>> result = new ArrayList<Tuple2<Integer, ImageSerializableWrapper>>();
					result.add(new Tuple2<Integer, ImageSerializableWrapper>(RasterizationUtils.Encode2DTo1DId(partitionX, partitionY, partitionCoordinateX, partitionCoordinateY),new ImageSerializableWrapper(imagePartition)));
					return result.iterator();
				}		
			});
		}
		else
		{
			BufferedImage renderedImage = new BufferedImage(this.resolutionX,this.resolutionY,BufferedImage.TYPE_INT_ARGB);
			List<Tuple2<Integer,Color>> colorMatrix = this.distributedColorMatrix.collect();
			for(Tuple2<Integer,Color> pixelColor:colorMatrix)
			{
				int pixelX = RasterizationUtils.Decode1DTo2DId(this.resolutionX,this.resolutionY,pixelColor._1)._1;
				int pixelY = RasterizationUtils.Decode1DTo2DId(this.resolutionX,this.resolutionY,pixelColor._1)._2;
				renderedImage.setRGB(pixelX, (this.resolutionY-1)-pixelY, pixelColor._2.getRGB());
			}
			this.pixelImage = renderedImage;
		}
		logger.debug("[VisualizationOperator][RenderImage][Stop]");
		return true;
	}
	
	/**
	 * Stitch image partitions.
	 *
	 * @return true, if successful
	 * @throws Exception the exception
	 */
	public boolean stitchImagePartitions() throws Exception
	{
		if(this.distributedPixelImage==null)
		{
			throw new Exception("[VisualizationOperator][stitchImagePartitions] The distributed pixel image is null. No need to stitch.");
		}
		List<Tuple2<Integer, ImageSerializableWrapper>> imagePartitions = this.distributedPixelImage.collect();
		BufferedImage renderedImage = new BufferedImage(this.resolutionX,this.resolutionY,BufferedImage.TYPE_INT_ARGB);
		//Stitch all image partitions together
		for(int i=0;i<imagePartitions.size();i++)
		{
			BufferedImage imagePartition = imagePartitions.get(i)._2.image;
			Tuple2<Integer,Integer> partitionCoordinate = RasterizationUtils.Decode1DTo2DId(this.partitionX, this.partitionY, imagePartitions.get(i)._1);
			int partitionMinX = partitionCoordinate._1*this.partitionIntervalX;
			int partitionMinY = ((this.partitionY-1)-partitionCoordinate._2)*this.partitionIntervalY;
			int[] rgbArray = imagePartition.getRGB(0, 0, imagePartition.getWidth(), imagePartition.getHeight(), null, 0, imagePartition.getWidth());
			renderedImage.setRGB(partitionMinX, partitionMinY, partitionIntervalX, partitionIntervalY, rgbArray, 0, partitionIntervalX);
		}
		this.pixelImage = renderedImage;
		return true;
	}

	/**
	 * Customize color.
	 *
	 * @param red the red
	 * @param green the green
	 * @param blue the blue
	 * @param colorAlpha the color alpha
	 * @param controlColorChannel the control color channel
	 * @param useInverseRatioForControlColorChannel the use inverse ratio for control color channel
	 * @return true, if successful
	 */
	public boolean CustomizeColor(int red, int green, int blue, int colorAlpha, Color controlColorChannel, boolean useInverseRatioForControlColorChannel)
	{
		logger.debug("[VisualizationOperator][CustomizeColor][Start]");
		this.red = red;
		this.green = green;
		this.blue = blue;
		this.colorAlpha = colorAlpha;
		this.controlColorChannel= controlColorChannel;

		this.useInverseRatioForControlColorChannel = useInverseRatioForControlColorChannel;
		logger.debug("[VisualizationOperator][CustomizeColor][Stop]");
		return true;
	}
	
	/**
	 * Encode to color.
	 *
	 * @param normailizedCount the normailized count
	 * @return the color
	 * @throws Exception the exception
	 */
	protected Color EncodeToColor(int normailizedCount) throws Exception
	{
		if(controlColorChannel.equals(Color.RED))
		{
			red=useInverseRatioForControlColorChannel?255-normailizedCount:normailizedCount;
		}
		else if(controlColorChannel.equals(Color.GREEN))
		{
			green=useInverseRatioForControlColorChannel?255-normailizedCount:normailizedCount;
		}
		else if(controlColorChannel.equals(Color.BLUE))
		{
			blue=useInverseRatioForControlColorChannel?255-normailizedCount:normailizedCount;
		}
		else throw new Exception("[VisualizationOperator][GenerateColor] Unsupported changing color color type. It should be in R,G,B");
		
		if(normailizedCount==0)
		{
			return new Color(red,green,blue,0);
		}
		return new Color(red,green,blue,colorAlpha);
	}
	
	
	/**
	 * Rasterize.
	 *
	 * @param sparkContext the spark context
	 * @param spatialRDD the spatial RDD
	 * @param useSparkDefaultPartition the use spark default partition
	 * @return the java pair RDD
	 */
	protected JavaPairRDD<Integer, Long> Rasterize(JavaSparkContext sparkContext, 
			SpatialRDD spatialRDD,boolean useSparkDefaultPartition) {
		logger.debug("[VisualizationOperator][Rasterize][Start]");
		JavaRDD<Object> rawSpatialRDD = spatialRDD.rawSpatialRDD;
		JavaPairRDD<Integer, Long> spatialRDDwithPixelId = rawSpatialRDD.flatMapToPair(new PairFlatMapFunction<Object,Integer,Long>()
		{

			@Override
			public Iterator<Tuple2<Integer, Long>> call(Object spatialObject) throws Exception {
				if(spatialObject instanceof Point)
				{
					return RasterizationUtils.FindPixelCoordinates(resolutionX,resolutionY,datasetBoundary,(Point)spatialObject,reverseSpatialCoordinate).iterator();
				}
				else if(spatialObject instanceof Envelope)
				{
					return RasterizationUtils.FindPixelCoordinates(resolutionX,resolutionY,datasetBoundary,(Envelope)spatialObject,reverseSpatialCoordinate).iterator();
				}
				else if(spatialObject instanceof Polygon)
				{
					return RasterizationUtils.FindPixelCoordinates(resolutionX,resolutionY,datasetBoundary,(Polygon)spatialObject,reverseSpatialCoordinate).iterator();
				}
				else if(spatialObject instanceof LineString)
				{
					return RasterizationUtils.FindPixelCoordinates(resolutionX,resolutionY,datasetBoundary,(LineString)spatialObject,reverseSpatialCoordinate).iterator();
				}
				else
				{
					throw new Exception("[VisualizationOperator][Rasterize] Unsupported spatial object types.");
				}
			}
		});
		JavaPairRDD<Integer, Long> originalImage = sparkContext.parallelizePairs(this.countMatrix);
		spatialRDDwithPixelId = spatialRDDwithPixelId.union(originalImage);
		JavaPairRDD<Integer, Long> pixelWeights = spatialRDDwithPixelId.reduceByKey(new Function2<Long,Long,Long>()
		{
			@Override
			public Long call(Long count1, Long count2) throws Exception {
				if(cumulativeCount==true)
				{
					return count1+count2;
				}
				else return new Long(count1);
			}			
		});
		this.distributedCountMatrix = pixelWeights;
		logger.debug("[VisualizationOperator][Rasterize][Stop]");
		return this.distributedCountMatrix;
	}
	

	
	/**
	 * Rasterize.
	 *
	 * @param sparkContext the spark context
	 * @param spatialPairRDD the spatial pair RDD
	 * @param useSparkDefaultPartition the use spark default partition
	 * @return the java pair RDD
	 */
	protected JavaPairRDD<Integer, Long> Rasterize(JavaSparkContext sparkContext, 
			JavaPairRDD spatialPairRDD,boolean useSparkDefaultPartition) {
		logger.debug("[VisualizationOperator][Rasterize][Start]");
		JavaPairRDD<Integer, Long> spatialRDDwithPixelId = spatialPairRDD.flatMapToPair(new PairFlatMapFunction<Tuple2,Integer,Long>()
		{
			@Override
			public Iterator<Tuple2<Integer, Long>> call(Tuple2 spatialObject) throws Exception {
				if(spatialObject._1 instanceof Envelope)
				{
					return RasterizationUtils.FindPixelCoordinates(resolutionX,resolutionY,datasetBoundary,(Envelope)spatialObject._1(),reverseSpatialCoordinate,(Long)spatialObject._2()).iterator();

				}
				else
				{
					return RasterizationUtils.FindPixelCoordinates(resolutionX,resolutionY,datasetBoundary,(Polygon)spatialObject._1(),reverseSpatialCoordinate,(Long)spatialObject._2()).iterator();
				}
			}
		});
		JavaPairRDD<Integer, Long> originalImage = sparkContext.parallelizePairs(this.countMatrix);
		JavaPairRDD<Integer,Long> completeSpatialRDDwithPixelId = spatialRDDwithPixelId.union(originalImage);
		JavaPairRDD<Integer, Long> pixelWeights = completeSpatialRDDwithPixelId.reduceByKey(new Function2<Long,Long,Long>()
		{
			@Override
			public Long call(Long count1, Long count2) throws Exception {
				if(cumulativeCount==true)
				{
					return count1+count2;
				}
				else return new Long(count1>count2?count1:count2);
			}			
		});
		this.distributedCountMatrix = pixelWeights;
		logger.debug("[VisualizationOperator][Rasterize][Stop]");
		return this.distributedCountMatrix;
	}

}
