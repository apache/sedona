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

/**
 * The Class NativeJavaImageGenerator.
 */
public class NativeJavaImageGenerator extends ImageGenerator{

	/* (non-Javadoc)
	 * @see org.datasyslab.babylon.core.ImageGenerator#SaveAsFile(java.awt.image.BufferedImage, java.lang.String)
	 */
	@Override
	public boolean SaveAsFile(BufferedImage pixelImage, String outputPath) {
		File outputImage = new File(outputPath+".png");
		outputImage.getParentFile().mkdirs();
		try {
			ImageIO.write(pixelImage,"png",outputImage);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}


}
