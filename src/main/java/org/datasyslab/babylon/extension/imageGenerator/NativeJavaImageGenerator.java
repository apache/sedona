/**
 * FILE: NativeJavaImageGenerator.java
 * PATH: org.datasyslab.babylon.extension.imageGenerator.NativeJavaImageGenerator.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.extension.imageGenerator;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

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


}
