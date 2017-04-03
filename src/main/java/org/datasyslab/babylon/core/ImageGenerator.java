/**
 * FILE: ImageGenerator.java
 * PATH: org.datasyslab.babylon.core.ImageGenerator.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.core;

import java.awt.image.BufferedImage;
import java.io.Serializable;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.datasyslab.babylon.utils.ImageType;

import scala.Tuple2;

/**
 * The Class ImageGenerator.
 */
public abstract class ImageGenerator implements Serializable{

	/**
	 * Save as file.
	 *
	 * @param distributedPixelImage the distributed pixel image
	 * @param outputPath the output path
	 * @param imageType the image type
	 * @return true, if successful
	 * @throws Exception the exception
	 */
	public boolean SaveAsFile(JavaPairRDD<Integer,ImageSerializableWrapper> distributedPixelImage, String outputPath, ImageType imageType) throws Exception
	{
		List<Tuple2<Integer,ImageSerializableWrapper>> imagePartitions = distributedPixelImage.collect();
		for(Tuple2<Integer,ImageSerializableWrapper> imagePartition:imagePartitions)
		{
			this.SaveAsFile(imagePartition._2.image, outputPath+"-"+imagePartition._1, imageType);
		}
		return true;
	}
	
	/**
	 * Save as file.
	 *
	 * @param pixelImage the pixel image
	 * @param outputPath the output path
	 * @param imageType the image type
	 * @return true, if successful
	 * @throws Exception the exception
	 */
	public abstract boolean SaveAsFile(BufferedImage pixelImage, String outputPath, ImageType imageType) throws Exception;
}
