/**
 * FILE: VisualizationOperator.java
 * PATH: org.datasyslab.babylon.core.VisualizationOperator.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.core;

import java.awt.Color;
import java.awt.geom.Ellipse2D;
import java.awt.image.BufferedImage;
import java.io.Serializable;
import java.util.*;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import org.datasyslab.babylon.utils.ColorizeOption;
import org.datasyslab.babylon.utils.Pixel;
import org.datasyslab.babylon.utils.RasterizationUtils;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;

import org.jfree.graphics2d.svg.SVGGraphics2D;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

import scala.Tuple2;


class RasterPixelCountComparator implements Comparator<Tuple2<Pixel, Double>>, Serializable
{

	@Override
	public int compare(Tuple2<Pixel, Double> spatialObject1, Tuple2<Pixel, Double> spatialObject2) {
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

class VectorObjectCountComparator implements Comparator<Tuple2<Object, Double>>, Serializable
{

	@Override
	public int compare(Tuple2<Object, Double> spatialObject1, Tuple2<Object, Double> spatialObject2) {
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

class PixelSerialIdComparator implements Comparator<Tuple2<Pixel, Double>>
{

    @Override
	public int compare(Tuple2<Pixel, Double> spatialObject1, Tuple2<Pixel, Double> spatialObject2) {
        int spatialObject1id = spatialObject1._1().hashCode();
        int spatialObject2id = spatialObject2._1().hashCode();

        if(spatialObject1id>spatialObject2id)
		{
			return 1;
		}
		else if(spatialObject1id<spatialObject2id)
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
	
	/** The colorize option. Scatter plot can select uniform color or z-axis value option to visualize.
	 *  Heat map must select spatial aggregation.
	 * */
	protected ColorizeOption colorizeOption = ColorizeOption.UNIFORMCOLOR;
	/** The reverse spatial coordinate. */
	protected boolean reverseSpatialCoordinate; // Switch the x and y when draw the final image
	
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
	
	/*
	 * Parameters determine raster image
	 */
	
	/** The generate vector image. */
	protected boolean generateVectorImage=false;
	
	/** The count matrix. */
	protected List<Tuple2<Integer,Double>> countMatrix;
	
	/** The distributed count matrix. */
	protected JavaPairRDD<Pixel, Double> distributedRasterCountMatrix;
	
	/** The distributed color matrix. */
	protected JavaPairRDD<Pixel, Color> distributedRasterColorMatrix;
	
	/** The raster image. */
	public BufferedImage rasterImage=null;
	
	/** The distributed raster image. */
	public JavaPairRDD<Integer,ImageSerializableWrapper> distributedRasterImage=null;

	
	
	/** The distributed vector objects. */
	/*
	 * Parameters determine vector image
	 */
	protected JavaPairRDD<Object,Double> distributedVectorObjects;
	
	/** The distributed vector colors. */
	protected JavaPairRDD<Object,Color> distributedVectorColors;
	
	/** The only draw outline. */
	protected boolean onlyDrawOutline = true;
	
	/** The vector image. */
	public List<String> vectorImage=null;
	
	/** The distributed vector image. */
	// This pair RDD only contains three distinct keys: 0, 1, 2. 0 is the SVG file header, 1 is SVG file body, 2 is SVG file footer.
	public JavaPairRDD<Integer,String> distributedVectorImage=null; 
	
	
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
	protected boolean hasBeenSpatialPartitioned=false;
	
	/*
	 * Parameter tells whether do photo filter in parallel and do rendering in parallel
	 */
	/** The parallel photo filter. */
	protected boolean parallelPhotoFilter = false;
	
	/** The parallel render image. */
	protected boolean parallelRenderImage = false;
		
	/*
	 * Parameter for the overall system
	 */
	
	/** The Constant logger. */
	final static Logger logger = Logger.getLogger(VisualizationOperator.class);
	
	
	/**
	 * Instantiates a new visualization operator.
	 *
	 * @param resolutionX the resolution X
	 * @param resolutionY the resolution Y
	 * @param datasetBoundary the dataset boundary
	 * @param colorizeOption the colorize option
	 * @param reverseSpatialCoordinate the reverse spatial coordinate
	 * @param partitionX the partition X
	 * @param partitionY the partition Y
	 * @param parallelPhotoFilter the parallel photo filter
	 * @param parallelRenderImage the parallel render image
	 * @param generateVectorImage the generate vector image
	 */
	public VisualizationOperator(int resolutionX, int resolutionY,Envelope datasetBoundary, ColorizeOption colorizeOption, boolean reverseSpatialCoordinate,
			int partitionX, int partitionY, boolean parallelPhotoFilter, boolean parallelRenderImage, boolean generateVectorImage)
	{
		logger.info("[Babylon][Constructor][Start]");
		this.resolutionX = resolutionX;
		this.resolutionY = resolutionY;
		this.datasetBoundary = datasetBoundary;
		this.reverseSpatialCoordinate = reverseSpatialCoordinate;
		this.generateVectorImage=generateVectorImage;
		this.parallelRenderImage = parallelRenderImage;
		
		if(this.generateVectorImage) return;
		/*
		 * Variables below control how to initialize a raster image
		 */
		this.countMatrix = new ArrayList<Tuple2<Integer,Double>>();
		this.colorizeOption = colorizeOption;
		this.partitionX = partitionX;
		this.partitionY = partitionY;
		this.partitionIntervalX = this.resolutionX/this.partitionX;
		this.partitionIntervalY = this.resolutionY/this.partitionY;
		this.parallelPhotoFilter = parallelPhotoFilter;
		int serialId = 0;
		for(int j=0;j<resolutionY;j++)
		{
			for(int i=0;i<resolutionX;i++)
			{
				countMatrix.add(new Tuple2<Integer,Double>(serialId,new Double(0.0)));
				serialId++;
			}
		}
		logger.info("[Babylon][Constructor][Stop]");
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
	private JavaPairRDD<Pixel,Double> spatialPartitioningWithoutDuplicates() throws Exception
	{
		this.distributedRasterCountMatrix = this.distributedRasterCountMatrix.partitionBy(new VisualizationPartitioner(this.resolutionX,this.resolutionY,this.partitionX,this.partitionY));
		return this.distributedRasterCountMatrix;
	}

	private JavaPairRDD<Pixel,Double> spatialPartitioningWithDuplicates() throws Exception
	{
		this.distributedRasterCountMatrix = this.distributedRasterCountMatrix.flatMapToPair(new PairFlatMapFunction<Tuple2<Pixel, Double>, Pixel, Double>() {
			@Override
			public Iterator<Tuple2<Pixel, Double>> call(Tuple2<Pixel, Double> pixelDoubleTuple2) throws Exception {
				VisualizationPartitioner vizPartitioner = new VisualizationPartitioner(resolutionX,resolutionY,partitionX,partitionY);
				return vizPartitioner.getPartitionIDs(pixelDoubleTuple2, photoFilterRadius).iterator();
			}
		});
		this.distributedRasterCountMatrix = this.distributedRasterCountMatrix.partitionBy(new VisualizationPartitioner(this.resolutionX,this.resolutionY,this.partitionX,this.partitionY));
		return this.distributedRasterCountMatrix;
	}
	
	/**
	 * Apply photo filter.
	 *
	 * @param sparkContext the spark context
	 * @return the java pair RDD
	 * @throws Exception the exception
	 */
	protected JavaPairRDD<Pixel, Double> ApplyPhotoFilter(JavaSparkContext sparkContext) throws Exception
	{
		logger.info("[Babylon][ApplyPhotoFilter][Start]");
		if(this.parallelPhotoFilter)
		{
			if(this.hasBeenSpatialPartitioned==false)
			{
				this.spatialPartitioningWithDuplicates();
				this.hasBeenSpatialPartitioned = true;
			}
			this.distributedRasterCountMatrix = this.distributedRasterCountMatrix.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Pixel,Double>>,Pixel,Double>()
			{

				@Override
				public Iterator<Tuple2<Pixel, Double>> call(Iterator<Tuple2<Pixel, Double>> currentPartition) throws Exception  {
					// This function uses an efficient algorithm to recompute the pixel value. For each existing pixel,
					// we calculate its impact for all pixels within its impact range and add its impact values.
					HashMap<Pixel,Double> pixelCountHashMap = new HashMap<Pixel,Double>();
					while(currentPartition.hasNext())
					{
						Tuple2<Pixel, Double> currentPixelCount = currentPartition.next();
						Tuple2<Integer,Integer> centerPixelCoordinate = new Tuple2<Integer,Integer>(currentPixelCount._1().getX(),currentPixelCount._1().getY());
						// Find all pixels that are in the working pixel's impact range
						for (int x = -photoFilterRadius; x <= photoFilterRadius; x++) {
							for (int y = -photoFilterRadius; y <= photoFilterRadius; y++) {
								int neighborPixelX = centerPixelCoordinate._1+x;
								int neighborPixelY = centerPixelCoordinate._2+y;
								if(currentPixelCount._1().getCurrentPartitionId()<0)
								{
									throw new Exception("[VisualizationOperator][ApplyPhotoFilter] this pixel doesn't have currentPartitionId that is assigned in VisualizationPartitioner.");
								}
								if (neighborPixelX<0||neighborPixelX>resolutionX||neighborPixelY<0||neighborPixelY>resolutionY)
								{
                                    // This neighbor pixel is out of boundary so that we don't record its sum. We don't plot this pixel on the final sub image.
                                    continue;
                                }
								if(RasterizationUtils.CalculatePartitionId(resolutionX, resolutionY, partitionX, partitionY, neighborPixelX, neighborPixelY)!=currentPixelCount._1().getCurrentPartitionId())
								{
									// This neighbor pixel is not in this image partition so that we don't record its sum. We don't plot this pixel on the final sub image.
									continue;
								}
								Double neighborPixelCount = pixelCountHashMap.get(new Pixel(neighborPixelX, neighborPixelY,resolutionX,resolutionY));
								// For that pixel, sum up its new count
								if (neighborPixelCount!=null)
								{
									neighborPixelCount+=currentPixelCount._2()*PhotoFilterConvolutionMatrix[x + photoFilterRadius][y + photoFilterRadius];
									pixelCountHashMap.remove(new Pixel(neighborPixelX,neighborPixelY,resolutionX,resolutionY));
									Pixel newPixel = new Pixel(neighborPixelX, neighborPixelY, resolutionX,resolutionY,false, RasterizationUtils.CalculatePartitionId(resolutionX, resolutionY, partitionX, partitionY, neighborPixelX, neighborPixelY));
									pixelCountHashMap.put(newPixel,neighborPixelCount);
								}
								else
								{
									neighborPixelCount = currentPixelCount._2()*PhotoFilterConvolutionMatrix[x + photoFilterRadius][y + photoFilterRadius];
									Pixel newPixel = new Pixel(neighborPixelX, neighborPixelY, resolutionX,resolutionY,false, RasterizationUtils.CalculatePartitionId(resolutionX, resolutionY, partitionX, partitionY, neighborPixelX, neighborPixelY));
									pixelCountHashMap.put(newPixel,neighborPixelCount);
								}
							}
						}
					}
					// Loop over the result map and convert the map. This is not efficient and can be replaced by a better way.
					List<Tuple2<Pixel,Double>> resultSet = new ArrayList<Tuple2<Pixel,Double>>();
					Iterator<java.util.Map.Entry<Pixel, Double>> hashmapIterator = pixelCountHashMap.entrySet().iterator();
					while(hashmapIterator.hasNext())
					{
                        Map.Entry<Pixel,Double> cursorEntry = hashmapIterator.next();
					    resultSet.add(new Tuple2<Pixel, Double>(cursorEntry.getKey(),cursorEntry.getValue()));
					}
					return resultSet.iterator();
				}
			});
		}
		else
		{
			List<Tuple2<Pixel,Double>> collectedCountMatrix = this.distributedRasterCountMatrix.collect();
			HashMap<Pixel,Double> hashCountMatrix = new HashMap<Pixel,Double>();
			for(Tuple2<Pixel,Double> pixelCount:collectedCountMatrix)
			{
			    hashCountMatrix.put(pixelCount._1(),pixelCount._2());
            }
			//List<Tuple2<Pixel,Double>> originalCountMatrix = new ArrayList<Tuple2<Pixel,Double>>(collectedCountMatrix);
			//Collections.sort(originalCountMatrix, new PixelSerialIdComparator());
			final Broadcast<HashMap<Pixel,Double>> broadcastCountMatrix = sparkContext.broadcast(hashCountMatrix);
			this.distributedRasterCountMatrix = this.distributedRasterCountMatrix.mapToPair(new PairFunction<Tuple2<Pixel,Double>,Pixel,Double>()
			{
				@Override
				public Tuple2<Pixel, Double> call(Tuple2<Pixel, Double> pixel) throws Exception {
					Tuple2<Integer,Integer> centerPixelCoordinate = new Tuple2<Integer, Integer>(pixel._1().getX(),pixel._1().getY());
					Double pixelCount = new Double(0.0);
					for (int x = -photoFilterRadius; x <= photoFilterRadius; x++) {
						for (int y = -photoFilterRadius; y <= photoFilterRadius; y++) {
							int neighborPixelX = centerPixelCoordinate._1+x;
							int neighborPixelY = centerPixelCoordinate._2+y;
							if(neighborPixelX<resolutionX&&neighborPixelX>=0&&neighborPixelY<resolutionY&&neighborPixelY>=0)
							{
								Double neighborPixelCount = broadcastCountMatrix.getValue().get(new Pixel(neighborPixelX,neighborPixelY,resolutionX,resolutionY));
								if (neighborPixelCount==null)
								{
								    // Some of its neighbors are blank.
								    neighborPixelCount=0.0;
                                }
								pixelCount+=neighborPixelCount*PhotoFilterConvolutionMatrix[x + photoFilterRadius][y + photoFilterRadius];
							}
						}
					}
					return new Tuple2<Pixel,Double>(pixel._1,pixelCount);
				}				
			});
		}
		logger.info("[Babylon][ApplyPhotoFilter][Stop]");
		return this.distributedRasterCountMatrix;
	}
	
	
	/**
	 * Generate color matrix.
	 *
	 * @return the java pair RDD
	 */
	protected boolean Colorize()
	{
		logger.info("[Babylon][Colorize][Start]");

		if(this.generateVectorImage)
		{
			final Double maxWeight = this.distributedVectorObjects.max(new VectorObjectCountComparator())._2;
			final Double minWeight = 0.0;
			this.distributedVectorColors = this.distributedVectorObjects.mapValues(new Function<Double,Color>()
			{

				@Override
				public Color call(Double objectCount) throws Exception {
					
					Long normalizedObjectCount = new Double((objectCount-minWeight)*255/(maxWeight-minWeight)).longValue();
					Color objectColor = EncodeToColor(normalizedObjectCount.intValue());
					return objectColor;
				}
				
			});
		}
		else
		{
			final Double maxWeight = this.distributedRasterCountMatrix.max(new RasterPixelCountComparator())._2;
			final Double minWeight = 0.0;
			this.distributedRasterColorMatrix = this.distributedRasterCountMatrix.mapValues(new Function<Double, Color>()
			{

				@Override
				public Color call(Double pixelCount) throws Exception {
					Double normalizedPixelCount = (pixelCount-minWeight)*255/(maxWeight-minWeight);
					Color pixelColor = EncodeToColor(normalizedPixelCount.intValue());
					return pixelColor;
				}
			});
		}

		logger.info("[Babylon][Colorize][Stop]");
		return true;
	}
	
	/**
	 * Render image.
	 *
	 * @param sparkContext the spark context
	 * @return true, if successful
	 * @throws Exception the exception
	 */
	protected boolean RenderImage(JavaSparkContext sparkContext) throws Exception
	{
		logger.info("[Babylon][RenderImage][Start]");
		if(this.generateVectorImage)
		{
			this.distributedVectorImage = this.distributedVectorColors.mapToPair(new PairFunction<Tuple2<Object,Color>, Integer, String>()
			{

				@Override
				public Tuple2<Integer,String> call(Tuple2<Object, Color> color) throws Exception {
			        SVGGraphics2D g2 = new SVGGraphics2D(resolutionX, resolutionY);
					if(color._1() instanceof Point)
			        {
			        	g2.setPaint(color._2());
			            Ellipse2D circle = new Ellipse2D.Double(((Point)color._1()).getCoordinate().x, ((Point)color._1()).getCoordinate().y, 1, 1);
			        	g2.fill(circle);
			        	String svgBody = g2.getSVGBody();
			        	return new Tuple2<Integer,String>(1, svgBody);
			        }
			        else if(color._1() instanceof Polygon)
			        {
			        	g2.setPaint(color._2());
		        		Coordinate[] coordinates = ((Polygon)color._1()).getCoordinates();
		        		/*
		        		 * JTS polygon has one extra coordinate at the end. This coordinate is same with the first coordinate.
		        		 * But AWT.polygon does not need this extra coordinate.
		        		 */
		        		int[] xPoints = new int[coordinates.length-1];
		        		int[] yPoints = new int[coordinates.length-1];
		        		for(int i=0; i<coordinates.length-1;i++)
		        		{
		        			xPoints[i] = (int) coordinates[i].x;
		        			yPoints[i] = (int) coordinates[i].y;
		        		}
			        	if(onlyDrawOutline)
			        	{
			        		g2.drawPolygon(xPoints, yPoints, coordinates.length-1);
			        	}
			        	else
			        	{
			        		g2.fillPolygon(xPoints, yPoints, coordinates.length-1);
			        	}
			        	return new Tuple2<Integer,String>(1, g2.getSVGBody());
			        }
			        else if(color._1() instanceof LineString)
			        {
			        	g2.setPaint(color._2());
		        		Coordinate[] coordinates = ((LineString)color._1()).getCoordinates();
		        		int[] xPoints = new int[coordinates.length];
		        		int[] yPoints = new int[coordinates.length];
		        		for(int i=0; i<coordinates.length;i++)
		        		{
		        			xPoints[i] = (int) coordinates[i].x;
		        			yPoints[i] = (int) coordinates[i].y;
		        		}
		        		g2.drawPolyline(xPoints, yPoints, coordinates.length);
			        	return new Tuple2<Integer,String>(1, g2.getSVGBody());
			        }
			        else
			        {
						throw new Exception("[Babylon][RenderImage] Unsupported spatial object types. Babylon only supports Point, Polygon, LineString");
			        }
				}
			});
			/*
			 * Add SVG xml file header and footer to the distributed svg file
			 */
			List<Tuple2<Integer, String>> svgHeaderFooter = new ArrayList<Tuple2<Integer, String>>();
	        SVGGraphics2D g2 = new SVGGraphics2D(resolutionX, resolutionY);
			svgHeaderFooter.add(new Tuple2<Integer, String>(0,g2.getSVGHeader()));
			svgHeaderFooter.add(new Tuple2<Integer, String>(2,g2.getSVGFooter()));
			JavaPairRDD<Integer, String> distributedSVGHeaderFooter = sparkContext.parallelizePairs(svgHeaderFooter);
			this.distributedVectorImage = this.distributedVectorImage.union(distributedSVGHeaderFooter);
			this.distributedVectorImage = this.distributedVectorImage.sortByKey();
			if(this.parallelRenderImage==true)
			{ 
				return true;
			}
			
			JavaRDD<String> distributedVectorImageNoKey= this.distributedVectorImage.map(new Function<Tuple2<Integer,String>, String>()
			{

				@Override
				public String call(Tuple2<Integer, String> vectorObject) throws Exception {
					return vectorObject._2();
				}
				
			});
			this.vectorImage = distributedVectorImageNoKey.collect();
		}
		else if(this.parallelRenderImage==true && this.generateVectorImage==false)
		{
			if(this.hasBeenSpatialPartitioned==false)
			{
				this.spatialPartitioningWithoutDuplicates();
				this.hasBeenSpatialPartitioned = true;
			}
			this.distributedRasterImage = this.distributedRasterColorMatrix.mapPartitionsToPair(
					new PairFlatMapFunction<Iterator<Tuple2<Pixel,Color>>,Integer,ImageSerializableWrapper>()
			{
				@Override
				public Iterator<Tuple2<Integer, ImageSerializableWrapper>> call(Iterator<Tuple2<Pixel, Color>> currentPartition)
						throws Exception {
					BufferedImage imagePartition = new BufferedImage(partitionIntervalX,partitionIntervalY,BufferedImage.TYPE_INT_ARGB);
					Tuple2<Pixel,Color> pixelColor=null;
					Tuple2<Integer,Integer> pixelCoordinate = null;
					while(currentPartition.hasNext())
					{
						//Render color in this image partition pixel-wise.
						pixelColor = currentPartition.next();
						pixelCoordinate =  new Tuple2<Integer, Integer>(pixelColor._1().getX(),pixelColor._1().getY());
						imagePartition.setRGB(pixelCoordinate._1%partitionIntervalX, (partitionIntervalY -1)-pixelCoordinate._2%partitionIntervalY, pixelColor._2.getRGB());
					}
                    List<Tuple2<Integer, ImageSerializableWrapper>> result = new ArrayList<Tuple2<Integer, ImageSerializableWrapper>>();
                    if (pixelCoordinate==null)
					{
					    // No pixels in this partition. Skip this subimage
                        return result.iterator();
                    }
					int partitionCoordinateX = pixelCoordinate._1/partitionIntervalX;
					int partitionCoordinateY = pixelCoordinate._2/partitionIntervalY;
					result.add(new Tuple2<Integer, ImageSerializableWrapper>(RasterizationUtils.Encode2DTo1DId(partitionX, partitionY, partitionCoordinateX, partitionCoordinateY),new ImageSerializableWrapper(imagePartition)));
					return result.iterator();
				}
			});
		}
		else if(this.parallelRenderImage==false && this.generateVectorImage==false)
		{
			BufferedImage renderedImage = new BufferedImage(this.resolutionX,this.resolutionY,BufferedImage.TYPE_INT_ARGB);
			List<Tuple2<Pixel,Color>> colorMatrix = this.distributedRasterColorMatrix.collect();
			for(Tuple2<Pixel,Color> pixelColor:colorMatrix)
			{
				int pixelX = pixelColor._1().getX();
				int pixelY = pixelColor._1().getY();
				renderedImage.setRGB(pixelX, (this.resolutionY-1)-pixelY, pixelColor._2.getRGB());
			}
			this.rasterImage = renderedImage;
		}
		logger.info("[Babylon][RenderImage][Stop]");
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
		logger.info("[Babylon][stitchImagePartitions][Start]");
		if(this.distributedRasterImage==null)
		{
			throw new Exception("[Babylon][stitchImagePartitions] The distributed pixel image is null. No need to stitch.");
		}
		List<Tuple2<Integer, ImageSerializableWrapper>> imagePartitions = this.distributedRasterImage.collect();
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
		this.rasterImage = renderedImage;
		logger.info("[Babylon][stitchImagePartitions][Stop]");
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
		logger.info("[Babylon][CustomizeColor][Start]");
		this.red = red;
		this.green = green;
		this.blue = blue;
		this.colorAlpha = colorAlpha;
		this.controlColorChannel= controlColorChannel;

		this.useInverseRatioForControlColorChannel = useInverseRatioForControlColorChannel;
		logger.info("[Babylon][CustomizeColor][Stop]");
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
		else throw new Exception("[Babylon][GenerateColor] Unsupported changing color color type. It should be in R,G,B");
		
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
	protected JavaPairRDD<Pixel, Double> Rasterize(JavaSparkContext sparkContext,
			SpatialRDD spatialRDD,boolean useSparkDefaultPartition) {
		logger.info("[Babylon][Rasterize][Start]");
		JavaRDD<Object> rawSpatialRDD = spatialRDD.rawSpatialRDD;
		if(generateVectorImage)
		{
				this.distributedVectorObjects = rawSpatialRDD.flatMapToPair(new PairFlatMapFunction<Object,Object, Double>(){
					@Override
					public Iterator<Tuple2<Object,Double>> call(Object spatialObject) throws Exception {
						GeometryFactory geometryFactory = new GeometryFactory();
						List<Tuple2<Object,Double>> result = new ArrayList<Tuple2<Object,Double>>();
						if(spatialObject instanceof Point)
						{
							if(!datasetBoundary.covers(((Point) spatialObject).getEnvelopeInternal())) return result.iterator();							
							Coordinate coordinate =  RasterizationUtils.FindPixelCoordinates(resolutionX,resolutionY,datasetBoundary,((Point) spatialObject).getCoordinate(),reverseSpatialCoordinate,false,true);
							Point rasterizedObject = geometryFactory.createPoint(coordinate);
							if(colorizeOption==ColorizeOption.ZAXIS)
							{
								result.add(new Tuple2<Object,Double>(rasterizedObject,((Point) spatialObject).getCoordinate().z));
							}
							else
							{
								result.add(new Tuple2<Object,Double>(rasterizedObject,new Double(1.0)));
							}
							return result.iterator();
						}
						else if(spatialObject instanceof Polygon)
						{
							if(!datasetBoundary.covers(((Polygon) spatialObject).getEnvelopeInternal())) return result.iterator();							
							Coordinate[] spatialCoordinates = RasterizationUtils.FindPixelCoordinates(resolutionX,resolutionY,datasetBoundary,((Polygon) spatialObject).getCoordinates(),reverseSpatialCoordinate,false,true);
							result.add(new Tuple2<Object,Double>(geometryFactory.createPolygon(spatialCoordinates),new Double(1.0))); 
							return result.iterator();
						}
						else if(spatialObject instanceof LineString)
						{
							if(!datasetBoundary.covers(((LineString) spatialObject).getEnvelopeInternal())) return result.iterator();							
							Coordinate[] spatialCoordinates = RasterizationUtils.FindPixelCoordinates(resolutionX,resolutionY,datasetBoundary,((LineString) spatialObject).getCoordinates(),reverseSpatialCoordinate,false,true);
							result.add(new Tuple2<Object,Double>(geometryFactory.createLineString(spatialCoordinates), new Double(1.0))); 
							return result.iterator();
						}
						else
						{
							throw new Exception("[Babylon][Rasterize] Unsupported spatial object types. Babylon only supports Point, Polygon, LineString");
						}
					}});

		}
		else{
		JavaPairRDD<Pixel, Double> spatialRDDwithPixelId = rawSpatialRDD.flatMapToPair(new PairFlatMapFunction<Object,Pixel,Double>()
		{
			@Override
			public Iterator<Tuple2<Pixel, Double>> call(Object spatialObject) throws Exception {

					if(spatialObject instanceof Point)
					{
						return RasterizationUtils.FindPixelCoordinates(resolutionX,resolutionY,datasetBoundary,(Point)spatialObject,colorizeOption,reverseSpatialCoordinate).iterator();
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
						throw new Exception("[Babylon][Rasterize] Unsupported spatial object types. Babylon only supports Point, Polygon, LineString");
					}
			}
		});
		//JavaPairRDD<Integer, Double> originalImage = sparkContext.parallelizePairs(this.countMatrix);
		//spatialRDDwithPixelId = spatialRDDwithPixelId.union(originalImage);


            spatialRDDwithPixelId = spatialRDDwithPixelId.filter(new Function<Tuple2<Pixel, Double>, Boolean>() {
                @Override
                public Boolean call(Tuple2<Pixel, Double> pixelCount) throws Exception {
                    if (pixelCount._1().getX() < 0 || pixelCount._1().getX() > resolutionX || pixelCount._1().getY() < 0 || pixelCount._1().getY() > resolutionY)
                    {
                        return false;
                    }
                    else
                    {
                        return true;
                    }
                }
            });
            JavaPairRDD<Pixel, Double> pixelWeights = spatialRDDwithPixelId.reduceByKey(new Function2<Double,Double,Double>()
		{
			@Override
			public Double call(Double count1, Double count2) throws Exception {
				if(colorizeOption==ColorizeOption.SPATIALAGGREGATION)
				{
					return count1+count2;
				}
				else 
				{
					//TODO, colorizeOption for uniform color and z-axis color follow the same aggregate strategy 
					// which takes the large value. We need to find a better strategy to distinguish them.
					return count1>count2?count1:count2;
				}	
			}			
		});
		this.distributedRasterCountMatrix = pixelWeights;
		}
		logger.info("[Babylon][Rasterize][Stop]");
		return this.distributedRasterCountMatrix;
	}
	

	
	/**
	 * Rasterize.
	 *
	 * @param sparkContext the spark context
	 * @param spatialPairRDD the spatial pair RDD
	 * @param useSparkDefaultPartition the use spark default partition
	 * @return the java pair RDD
	 */
	protected JavaPairRDD<Pixel, Double> Rasterize(JavaSparkContext sparkContext,
			JavaPairRDD<Polygon,Long> spatialPairRDD,boolean useSparkDefaultPartition) {
		logger.info("[Babylon][Rasterize][Start]");
		if(generateVectorImage)
		{
			this.onlyDrawOutline = false;
			this.distributedVectorObjects = spatialPairRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<Polygon,Long>,Object,Double>(){
				@Override
				public Iterator<Tuple2<Object, Double>> call(Tuple2<Polygon, Long> spatialObject) throws Exception {
					GeometryFactory geometryFactory = new GeometryFactory();
					List<Tuple2<Object,Double>> result = new ArrayList<Tuple2<Object,Double>>();
					if(!datasetBoundary.covers(((Polygon) spatialObject._1()).getEnvelopeInternal())) return result.iterator();							
					Coordinate[] spatialCoordinates = RasterizationUtils.FindPixelCoordinates(resolutionX,resolutionY,datasetBoundary,((Polygon) spatialObject._1()).getCoordinates(),reverseSpatialCoordinate,false,true);
					result.add(new Tuple2<Object,Double>(geometryFactory.createPolygon(spatialCoordinates),new Double(spatialObject._2())));
					return result.iterator();
				}});
		}
		else
		{
			JavaPairRDD<Pixel, Double> spatialRDDwithPixelId = spatialPairRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<Polygon,Long>,Pixel,Double>()
			{
				@Override
				public Iterator<Tuple2<Pixel, Double>> call(Tuple2<Polygon,Long> spatialObject) throws Exception {
						return RasterizationUtils.FindPixelCoordinates(resolutionX,resolutionY,datasetBoundary,(Polygon)spatialObject._1(),reverseSpatialCoordinate,new Double(spatialObject._2())).iterator();
				}
			});
			//JavaPairRDD<Pixel, Double> originalImage = sparkContext.parallelizePairs(this.countMatrix);
			//JavaPairRDD<Integer,Double> completeSpatialRDDwithPixelId = spatialRDDwithPixelId.union(originalImage);
            spatialRDDwithPixelId = spatialRDDwithPixelId.filter(new Function<Tuple2<Pixel, Double>, Boolean>() {
                @Override
                public Boolean call(Tuple2<Pixel, Double> pixelCount) throws Exception {
                    if (pixelCount._1().getX() < 0 || pixelCount._1().getX() > resolutionX || pixelCount._1().getY() < 0 || pixelCount._1().getY() > resolutionY)
                    {
                        return false;
                    }
                    else
                    {
                        return true;
                    }
                }
            });
			JavaPairRDD<Pixel, Double> pixelWeights = spatialRDDwithPixelId.reduceByKey(new Function2<Double,Double,Double>()
			{
				@Override
				public Double call(Double count1, Double count2) throws Exception {
					if(colorizeOption==ColorizeOption.SPATIALAGGREGATION)
					{
						return count1+count2;
					}
					else {
						return count1>count2?count1:count2;
					}
				}		
			});
			this.distributedRasterCountMatrix = pixelWeights;
		}
		logger.info("[Babylon][Rasterize][Stop]");
		return this.distributedRasterCountMatrix;
	}

}
