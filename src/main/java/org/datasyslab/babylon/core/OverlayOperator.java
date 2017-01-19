/**
 * FILE: OverlayOperator.java
 * PATH: org.datasyslab.babylon.core.OverlayOperator.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.core;

import java.awt.Graphics;
import java.awt.image.BufferedImage;
import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * The Class OverlayOperator.
 */
public class OverlayOperator {
	
	/** The back image. */
	public BufferedImage backImage;
	
	/** The distributed back image. */
	public JavaPairRDD<Integer,BufferedImage> distributedBackImage;
	
	/**
	 * Instantiates a new overlay operator.
	 *
	 * @param distributedBackImage the distributed back image
	 */
	public OverlayOperator(JavaPairRDD<Integer,BufferedImage> distributedBackImage)
	{
		this.distributedBackImage = distributedBackImage;
	}
	
	/**
	 * Instantiates a new overlay operator.
	 *
	 * @param backImage the back image
	 */
	public OverlayOperator(BufferedImage backImage)
	{
		this.backImage = backImage;
	}
	
	/**
	 * Join image.
	 *
	 * @param distributedFontImage the distributed font image
	 * @return true, if successful
	 */
	public boolean JoinImage(JavaPairRDD<Integer,BufferedImage> distributedFontImage)
	{
		this.distributedBackImage = this.distributedBackImage.cogroup(distributedFontImage).mapToPair(new PairFunction<Tuple2<Integer,Tuple2<Iterable<BufferedImage>,Iterable<BufferedImage>>>,Integer,BufferedImage>()
		{
			@Override
			public Tuple2<Integer, BufferedImage> call(
					Tuple2<Integer, Tuple2<Iterable<BufferedImage>, Iterable<BufferedImage>>> imagePair)
						throws Exception {
				int imagePartitionId = imagePair._1;
				Iterator<BufferedImage> backImageIterator = imagePair._2._1.iterator();
				Iterator<BufferedImage> frontImageIterator = imagePair._2._2.iterator();
				if(backImageIterator.hasNext()==false)
				{
					throw new Exception("[OverlayOperator][JoinImage] The back image iterator didn't get any image partitions.");
				}
				if(frontImageIterator.hasNext()==false)
				{
					throw new Exception("[OverlayOperator][JoinImage] The front image iterator didn't get any image partitions.");
				}
				BufferedImage backImage = backImageIterator.next();
				BufferedImage frontImage = frontImageIterator.next();
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
				return new Tuple2(imagePartitionId,combinedImage);
			}
		});
		return true;
	}
	
	
	/**
	 * Join image.
	 *
	 * @param frontImage the front image
	 * @return true, if successful
	 * @throws Exception the exception
	 */
	public boolean JoinImage(BufferedImage frontImage) throws Exception
	{
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
		this.backImage = combinedImage;
		return true;
	}
}
