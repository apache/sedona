/**
 * FILE: BabylonImageGenerator.java
 * PATH: org.datasyslab.babylon.extension.imageGenerator.BabylonImageGenerator.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.extension.imageGenerator;

import java.awt.image.BufferedImage;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import javax.imageio.ImageIO;

import org.apache.log4j.Logger;
import org.datasyslab.babylon.core.ImageGenerator;
import org.datasyslab.babylon.utils.ImageType;

// TODO: Auto-generated Javadoc
/**
 * The Class BabylonImageGenerator.
 */
public class BabylonImageGenerator extends ImageGenerator{

	/** The Constant logger. */
	final static Logger logger = Logger.getLogger(BabylonImageGenerator.class);
	/* (non-Javadoc)
	 * @see org.datasyslab.babylon.core.AbstractImageGenerator#SaveRasterImageAsLocalFile(java.awt.image.BufferedImage, java.lang.String, org.datasyslab.babylon.utils.ImageType)
	 */
	public boolean SaveRasterImageAsLocalFile(BufferedImage rasterImage, String outputPath, ImageType imageType) throws Exception
	{
		logger.info("[Babylon][SaveRasterImageAsLocalFile][Start]");
		File outputImage = new File(outputPath+"."+imageType.getTypeName());
		outputImage.getParentFile().mkdirs();
		try {
			ImageIO.write(rasterImage,imageType.getTypeName(),outputImage);
		} catch (IOException e) {
			e.printStackTrace();
		}
		logger.info("[Babylon][SaveRasterImageAsLocalFile][Stop]");
		return true;
	}
	
	/* (non-Javadoc)
	 * @see org.datasyslab.babylon.core.AbstractImageGenerator#SaveVectorImageAsLocalFile(java.util.List, java.lang.String, org.datasyslab.babylon.utils.ImageType)
	 */
	public boolean SaveVectorImageAsLocalFile(List<String> vectorImage, String outputPath, ImageType imageType) throws Exception
	{
		logger.info("[Babylon][SaveVectorImageAsLocalFile][Start]");
		File outputImage = new File(outputPath+"."+imageType.getTypeName());
		outputImage.getParentFile().mkdirs();
		
		BufferedWriter bw = null;
		FileWriter fw = null;
		try {
			fw = new FileWriter(outputImage);
			bw = new BufferedWriter(fw);
			for(String svgElement : vectorImage)
			{
				bw.write(svgElement);
			}

		} catch (IOException e) {

			e.printStackTrace();

		} finally {

			try {

				if (bw != null)
					bw.close();

				if (fw != null)
					fw.close();

			} catch (IOException ex) {

				ex.printStackTrace();

			}
		}
		logger.info("[Babylon][SaveVectorImageAsLocalFile][Stop]");
		return true;
	}
}
