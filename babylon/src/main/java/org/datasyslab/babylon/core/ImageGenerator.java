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


public abstract class ImageGenerator implements Serializable{


	/**
	 * Save raster image as spark file.
	 *
	 * @param distributedImage the distributed image
	 * @param outputPath the output path
	 * @param imageType the image type
	 * @return true, if successful
	 * @throws Exception the exception
	 */
	public boolean SaveRasterImageAsSparkFile(JavaPairRDD<Integer,ImageSerializableWrapper> distributedImage, String outputPath, ImageType imageType) throws Exception
	{
		
		distributedImage.saveAsObjectFile(outputPath+"."+imageType.getTypeName());
		return true;
	}
	
	/**
	 * Save raster image as local file.
	 *
	 * @param distributedImage the distributed image
	 * @param outputPath the output path
	 * @param imageType the image type
	 * @return true, if successful
	 * @throws Exception the exception
	 */
	public boolean SaveRasterImageAsLocalFile(JavaPairRDD<Integer,ImageSerializableWrapper> distributedImage, String outputPath, ImageType imageType) throws Exception
	{
		List<Tuple2<Integer,ImageSerializableWrapper>> imagePartitions = distributedImage.collect();
		for(Tuple2<Integer,ImageSerializableWrapper> imagePartition:imagePartitions)
		{
			SaveRasterImageAsLocalFile(imagePartition._2.image, outputPath+"-"+imagePartition._1, imageType);
		}
		return true;
	}
	
	/**
	 * Save raster image as local file.
	 *
	 * @param rasterImage the raster image
	 * @param outputPath the output path
	 * @param imageType the image type
	 * @return true, if successful
	 * @throws Exception the exception
	 */
	public abstract boolean SaveRasterImageAsLocalFile(BufferedImage rasterImage, String outputPath, ImageType imageType) throws Exception;

	
	/**
	 * Save vector image as spark file.
	 *
	 * @param distributedImage the distributed image
	 * @param outputPath the output path
	 * @param imageType the image type
	 * @return true, if successful
	 * @throws Exception the exception
	 */
	public boolean SaveVectorImageAsSparkFile(JavaPairRDD<Integer,String> distributedImage, String outputPath, ImageType imageType) throws Exception
	{
		
		distributedImage.saveAsTextFile(outputPath+"."+imageType.getTypeName());
		return true;
	}
	
	/**
	 * Save vectormage as local file.
	 *
	 * @param distributedImage the distributed image
	 * @param outputPath the output path
	 * @param imageType the image type
	 * @return true, if successful
	 * @throws Exception the exception
	 */
	public boolean SaveVectormageAsLocalFile(JavaPairRDD<Integer,String> distributedImage, String outputPath, ImageType imageType) throws Exception
	{
		JavaRDD<String> distributedVectorImageNoKey= distributedImage.map(new Function<Tuple2<Integer,String>, String>()
		{

			@Override
			public String call(Tuple2<Integer, String> vectorObject) throws Exception {
				return vectorObject._2();
			}
			
		});
		this.SaveVectorImageAsLocalFile(distributedVectorImageNoKey.collect(), outputPath, imageType);
		return true;
	}
	
	/**
	 * Save vector image as local file.
	 *
	 * @param vectorImage the vector image
	 * @param outputPath the output path
	 * @param imageType the image type
	 * @return true, if successful
	 * @throws Exception the exception
	 */
	public abstract boolean SaveVectorImageAsLocalFile(List<String> vectorImage, String outputPath, ImageType imageType) throws Exception;


}
