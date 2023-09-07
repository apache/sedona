/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sedona.viz.core;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.awt.Graphics;
import java.awt.image.BufferedImage;
import java.util.Iterator;

// TODO: Auto-generated Javadoc

/**
 * The Class RasterOverlayOperator.
 */
public class RasterOverlayOperator
{

    /**
     * The Constant logger.
     */
    final static Logger logger = Logger.getLogger(RasterOverlayOperator.class);
    /**
     * The back raster image.
     */
    public BufferedImage backRasterImage = null;
    /**
     * The distributed back raster image.
     */
    public JavaPairRDD<Integer, ImageSerializableWrapper> distributedBackRasterImage = null;
    /**
     * The generate distributed image.
     */
    public boolean generateDistributedImage = false;

    /**
     * Instantiates a new raster overlay operator.
     *
     * @param backRasterImage the back raster image
     */
    public RasterOverlayOperator(BufferedImage backRasterImage)
    {
        this.backRasterImage = backRasterImage;
        this.generateDistributedImage = false;
    }

    /**
     * Instantiates a new raster overlay operator.
     *
     * @param distributedBackRasterImage the distributed back raster image
     */
    public RasterOverlayOperator(JavaPairRDD<Integer, ImageSerializableWrapper> distributedBackRasterImage)
    {
        this.distributedBackRasterImage = distributedBackRasterImage;
        this.generateDistributedImage = true;
    }

    /**
     * Join image.
     *
     * @param distributedFontImage the distributed font image
     * @return true, if successful
     * @throws Exception the exception
     */
    public boolean JoinImage(JavaPairRDD<Integer, ImageSerializableWrapper> distributedFontImage)
            throws Exception
    {
        logger.info("[Sedona-Viz][JoinImage][Start]");
        if (this.generateDistributedImage == false) {
            throw new Exception("[OverlayOperator][JoinImage] The back image is not distributed. Please don't use distributed format.");
        }
        this.distributedBackRasterImage = this.distributedBackRasterImage.cogroup(distributedFontImage).mapToPair(new PairFunction<Tuple2<Integer, Tuple2<Iterable<ImageSerializableWrapper>, Iterable<ImageSerializableWrapper>>>, Integer, ImageSerializableWrapper>()
        {
            @Override
            public Tuple2<Integer, ImageSerializableWrapper> call(
                    Tuple2<Integer, Tuple2<Iterable<ImageSerializableWrapper>, Iterable<ImageSerializableWrapper>>> imagePair)
                    throws Exception
            {
                int imagePartitionId = imagePair._1;
                Iterator<ImageSerializableWrapper> backImageIterator = imagePair._2._1.iterator();
                Iterator<ImageSerializableWrapper> frontImageIterator = imagePair._2._2.iterator();
                if (backImageIterator.hasNext() == false) {
                    throw new Exception("[OverlayOperator][JoinImage] The back image iterator didn't get any image partitions.");
                }
                if (frontImageIterator.hasNext() == false) {
                    throw new Exception("[OverlayOperator][JoinImage] The front image iterator didn't get any image partitions.");
                }
                BufferedImage backImage = backImageIterator.next().image;
                BufferedImage frontImage = frontImageIterator.next().image;
                if (backImage.getWidth() != frontImage.getWidth() || backImage.getHeight() != frontImage.getHeight()) {
                    throw new Exception("[OverlayOperator][JoinImage] The two given image don't have the same width or the same height.");
                }
                int w = Math.max(backImage.getWidth(), frontImage.getWidth());
                int h = Math.max(backImage.getHeight(), frontImage.getHeight());
                BufferedImage combinedImage = new BufferedImage(w, h, BufferedImage.TYPE_INT_ARGB);
                Graphics graphics = combinedImage.getGraphics();
                graphics.drawImage(backImage, 0, 0, null);
                graphics.drawImage(frontImage, 0, 0, null);
                logger.info("[Sedona-Viz][JoinImage][Stop]");
                return new Tuple2<Integer, ImageSerializableWrapper>(imagePartitionId, new ImageSerializableWrapper(combinedImage));
            }
        });
        return true;
    }

    /**
     * Join image.
     *
     * @param frontRasterImage the front raster image
     * @return true, if successful
     * @throws Exception the exception
     */
    public boolean JoinImage(BufferedImage frontRasterImage)
            throws Exception
    {
        if (this.generateDistributedImage == true) {
            throw new Exception("[OverlayOperator][JoinImage] The back image is distributed. Please don't use centralized format.");
        }
        if (backRasterImage.getWidth() != frontRasterImage.getWidth() || backRasterImage.getHeight() != frontRasterImage.getHeight()) {
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
}
