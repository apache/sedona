/**
 * FILE: OverlayOperator.java
 * PATH: org.datasyslab.babylon.core.OverlayOperator.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.core;

import java.awt.Graphics;
import java.awt.image.BufferedImage;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * The Class OverlayOperator.
 */
public class OverlayOperator {
	
	/** The back image. */
	public BufferedImage backRasterImage=null;
	
	/** The back vector image. */
	public List<String> backVectorImage=null;
	
	/** The distributed back image. */
	public JavaPairRDD<Integer,ImageSerializableWrapper> distributedBackRasterImage=null;
	
	/** The distributed back vector image. */
	public JavaPairRDD<Integer,String> distributedBackVectorImage=null; 
	
	/** The generate vector image. */
	public boolean generateVectorImage=false;
	
	/** The generate distributed image. */
	public boolean generateDistributedImage=false;
	
	/**
	 * Instantiates a new overlay operator.
	 *
	 * @param distributedBackRasterImage the distributed back raster image
	 * @deprecated This constructor always overlays raster images. Please append one more parameter
	 * at the end to specify whether generate vector image or not.
	 */
	public OverlayOperator(JavaPairRDD<Integer,ImageSerializableWrapper> distributedBackRasterImage)
	{
		this.distributedBackRasterImage = distributedBackRasterImage;
		this.generateVectorImage=false;
		this.generateDistributedImage=true;
	}
	
	/**
	 * Instantiates a new overlay operator.
	 *
	 * @param distributedBackImage the distributed back image
	 * @param generateVectorImage the generate vector image
	 */
	public OverlayOperator(JavaPairRDD distributedBackImage,boolean generateVectorImage)
	{
		if(generateVectorImage)
		{
			this.distributedBackVectorImage = distributedBackImage;
			this.generateVectorImage=false;
			this.generateDistributedImage=true;
		}
		else
		{
			this.distributedBackRasterImage = distributedBackImage;
			this.generateVectorImage=false;
			this.generateDistributedImage=true;
		}
	}
	
	/**
	 * Instantiates a new overlay operator.
	 *
	 * @param backRasterImage the back raster image
	 */
	public OverlayOperator(BufferedImage backRasterImage)
	{
		this.backRasterImage = backRasterImage;
		this.generateVectorImage = false;
		this.generateDistributedImage = false;
	}
	
	/**
	 * Instantiates a new overlay operator.
	 *
	 * @param backRasterImage the back raster image
	 * @param generateVectorImage the generate vector image
	 */
	public OverlayOperator(BufferedImage backRasterImage, boolean generateVectorImage)
	{
		this.backRasterImage = backRasterImage;
		this.generateVectorImage = false;
		this.generateDistributedImage = false;
	}

	/**
	 * Instantiates a new overlay operator.
	 *
	 * @param backVectorImage the back vector image
	 */
	public OverlayOperator(List<String> backVectorImage)
	{
		this.backVectorImage = backVectorImage;
		this.generateVectorImage = true;
		this.generateDistributedImage = false;
	}
	
	/**
	 * Instantiates a new overlay operator.
	 *
	 * @param backVectorImage the back vector image
	 * @param generateVectorImage the generate vector image
	 */
	public OverlayOperator(List<String> backVectorImage, boolean generateVectorImage)
	{
		this.backVectorImage = backVectorImage;
		this.generateVectorImage = true;
		this.generateDistributedImage = false;
	}

	/**
	 * Join image.
	 *
	 * @param distributedFontImage the distributed font image
	 * @return true, if successful
	 */
	public boolean JoinImage(JavaPairRDD distributedFontImage)
	{
		if(generateVectorImage)
		{
			// Prune SVG header and footer because we only need one header and footer per SVG even if we merge two SVG images.
			JavaPairRDD<Integer,String> distributedFontImageNoHeaderFooter = distributedFontImage.filter(new Function<Tuple2<Integer,String>,Boolean>()
			{

				@Override
				public Boolean call(Tuple2<Integer, String> vectorObject) throws Exception {
					// Check whether the vectorObject's key is 1. 1 means this object is SVG body.
					// 0 means this object is SVG header, 2 means this object is SVG footer.
					if(vectorObject._1 != 1)
					{
						return false;
					}
					return true;
				}
				
			});
			this.distributedBackVectorImage = this.distributedBackVectorImage.union(distributedFontImageNoHeaderFooter);
			this.distributedBackVectorImage=this.distributedBackVectorImage.sortByKey();
		}
		else
		{
			this.distributedBackRasterImage = this.distributedBackRasterImage.cogroup(distributedFontImage).mapToPair(new PairFunction<Tuple2<Integer,Tuple2<Iterable<ImageSerializableWrapper>,Iterable<ImageSerializableWrapper>>>,Integer,ImageSerializableWrapper>()
			{
				@Override
				public Tuple2<Integer, ImageSerializableWrapper> call(
						Tuple2<Integer, Tuple2<Iterable<ImageSerializableWrapper>, Iterable<ImageSerializableWrapper>>> imagePair)
							throws Exception {
					int imagePartitionId = imagePair._1;
					Iterator<ImageSerializableWrapper> backImageIterator = imagePair._2._1.iterator();
					Iterator<ImageSerializableWrapper> frontImageIterator = imagePair._2._2.iterator();
					if(backImageIterator.hasNext()==false)
					{
						throw new Exception("[OverlayOperator][JoinImage] The back image iterator didn't get any image partitions.");
					}
					if(frontImageIterator.hasNext()==false)
					{
						throw new Exception("[OverlayOperator][JoinImage] The front image iterator didn't get any image partitions.");
					}
					BufferedImage backImage = backImageIterator.next().image;
					BufferedImage frontImage = frontImageIterator.next().image;
					if(backImage.getWidth()!=frontImage.getWidth()||backImage.getHeight()!=frontImage.getHeight())
					{
						throw new Exception("[OverlayOperator][JoinImage] The two given image don't have the same width or the same height.");
					}
					int w = Math.max(backImage.getWidth(), frontImage.getWidth());
					int h = Math.max(backImage.getHeight(), frontImage.getHeight());
					BufferedImage combinedImage = new BufferedImage(w, h, BufferedImage.TYPE_INT_ARGB);
					Graphics graphics = combinedImage.getGraphics();
					graphics.drawImage(backImage, 0, 0, null);
					graphics.drawImage(frontImage, 0, 0, null);
					return new Tuple2<Integer, ImageSerializableWrapper>(imagePartitionId,new ImageSerializableWrapper(combinedImage));
				}
			});
		}

		return true;
	}

	
	/**
	 * Join image.
	 *
	 * @param frontRasterImage the front raster image
	 * @return true, if successful
	 * @throws Exception the exception
	 */
	public boolean JoinImage(BufferedImage frontRasterImage) throws Exception
	{
		if(backRasterImage.getWidth()!=frontRasterImage.getWidth()||backRasterImage.getHeight()!=frontRasterImage.getHeight())
		{
			throw new Exception("[OverlayOperator][JoinImage] The two given image don't have the same width or the same height.");
		}
		int w = Math.max(backRasterImage.getWidth(), frontRasterImage.getWidth());
		int h = Math.max(backRasterImage.getHeight(), frontRasterImage.getHeight());
		BufferedImage combinedImage = new BufferedImage(w, h, BufferedImage.TYPE_INT_ARGB);
		Graphics graphics = combinedImage.getGraphics();
		graphics.drawImage(backRasterImage, 0, 0, null);
		graphics.drawImage(frontRasterImage, 0, 0, null);
		this.backRasterImage = combinedImage;
		return true;
	}
	
	/**
	 * Join image.
	 *
	 * @param frontVectorImage the front vector image
	 * @return true, if successful
	 * @throws Exception the exception
	 */
	public boolean JoinImage(List<String> frontVectorImage) throws Exception
	{
		// Merge two SVG images. Skip the first element and last element because they are SVG image header and footer.
		List<String> copyOf= new ArrayList<String>();
		for(int i=0;i<this.backVectorImage.size()-1;i++)
		{
			copyOf.add(this.backVectorImage.get(i));
		}
		for(int i=1;i<frontVectorImage.size()-1;i++)
		{
			copyOf.add(frontVectorImage.get(i));
		}
		copyOf.add(this.backVectorImage.get(this.backVectorImage.size()-1));
		this.backVectorImage = copyOf;
		return true;
	}
	
	/*
	public Object getOverlayedImage()
	{
		if(generateVectorImage && generateDistributedImage)
		{
			return this.distributedBackVectorImage;
		}
		else if(generateVectorImage==false && generateDistributedImage)
		{
			return this.distributedBackRasterImage;
		}
		else if(generateVectorImage && generateDistributedImage==false)
		{
			return this.backVectorImage;
		}
		else
		{
			return this.backRasterImage;
		}
	}
	*/
	
	
}
