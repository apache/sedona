/**
 * FILE: NativeJavaImageGenerator.java
 * PATH: org.datasyslab.babylon.extension.imageGenerator.NativeJavaImageGenerator.java
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

import org.datasyslab.babylon.core.ImageGenerator;
import org.datasyslab.babylon.utils.ImageType;

/**
 * The Class NativeJavaImageGenerator.
 */
public class NativeJavaImageGenerator extends ImageGenerator{

	/* (non-Javadoc)
	 * @see org.datasyslab.babylon.core.ImageGenerator#SaveAsFile(java.awt.image.BufferedImage, java.lang.String)
	 */
	@Override
	public boolean SaveAsFile(BufferedImage pixelImage, String outputPath, ImageType imageType) {
		File outputImage = new File(outputPath+"."+imageType.getTypeName());
		outputImage.getParentFile().mkdirs();
		try {
			ImageIO.write(pixelImage,imageType.getTypeName(),outputImage);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}

	/* (non-Javadoc)
	 * @see org.datasyslab.babylon.core.ImageGenerator#SaveAsFile(java.util.List, java.lang.String, org.datasyslab.babylon.utils.ImageType)
	 */
	@Override
	public boolean SaveAsFile(List<String> vectorImage, String outputPath, ImageType imageType) throws Exception {
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
		return true;
	}


}
