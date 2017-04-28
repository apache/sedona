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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.datasyslab.babylon.utils.ImageType;

import scala.Tuple2;

/**
 * The Class ImageGenerator.
 */
public abstract class ImageGenerator implements Serializable{

	/**
	 * Save as file.
	 *
	 * @param distributedImage the distributed image
	 * @param outputPath the output path
	 * @param imageType the image type
	 * @return true, if successful
	 * @throws Exception the exception
	 * @deprecated Old image generator has been deprecated. Please use BabylonImageGenerator instead.
	 */
	public boolean SaveAsFile(JavaPairRDD distributedImage, String outputPath, ImageType imageType) throws Exception
	{
		
		if(imageType.getTypeName().equalsIgnoreCase("svg"))
		{
			JavaRDD<String> distributedVectorImageNoKey= distributedImage.map(new Function<Tuple2<Integer,String>, String>()
			{

				@Override
				public String call(Tuple2<Integer, String> vectorObject) throws Exception {
					return vectorObject._2();
				}
				
			});
			this.SaveAsFile(distributedVectorImageNoKey.collect(), outputPath, imageType);
		}
		else
		{
			List<Tuple2<Integer,ImageSerializableWrapper>> imagePartitions = distributedImage.collect();
			for(Tuple2<Integer,ImageSerializableWrapper> imagePartition:imagePartitions)
			{
				this.SaveAsFile(imagePartition._2.image, outputPath+"-"+imagePartition._1, imageType);
			}
		}

		return true;
	}
	
	/**
	 * Save as file.
	 *
	 * @param rasterImage the raster image
	 * @param outputPath the output path
	 * @param imageType the image type
	 * @return true, if successful
	 * @throws Exception the exception
	 * @deprecated Old image generator has been deprecated. Please use BabylonImageGenerator instead.
	 */
	public abstract boolean SaveAsFile(BufferedImage rasterImage, String outputPath, ImageType imageType) throws Exception;
	
	/**
	 * Save as file.
	 *
	 * @param vectorImage the vector image
	 * @param outputPath the output path
	 * @param imageType the image type
	 * @return true, if successful
	 * @throws Exception the exception
	 * @deprecated Old image generator has been deprecated. Please use BabylonImageGenerator instead.
	 */
	public abstract boolean SaveAsFile(List<String> vectorImage, String outputPath, ImageType imageType) throws Exception;

}
