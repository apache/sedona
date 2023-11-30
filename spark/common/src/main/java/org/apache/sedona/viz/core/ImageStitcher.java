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

import com.amazonaws.services.s3.model.AmazonS3Exception;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.sedona.viz.utils.ImageType;
import org.apache.sedona.viz.utils.RasterizationUtils;
import org.apache.sedona.viz.utils.S3Operator;
import scala.Tuple2;

import javax.imageio.ImageIO;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

// TODO: Auto-generated Javadoc

/**
 * The Class ImageStitcher.
 */
public class ImageStitcher
{

    /**
     * The Constant logger.
     */
    final static Logger logger = Logger.getLogger(ImageStitcher.class);

    /**
     * Stitch image partitions from local file.
     *
     * @param imageTilePath the image tile path
     * @param resolutionX the resolution X
     * @param resolutionY the resolution Y
     * @param zoomLevel the zoom level
     * @param partitionOnX the partition on X
     * @param partitionOnY the partition on Y
     * @return true, if successful
     * @throws Exception the exception
     */
    public static boolean stitchImagePartitionsFromLocalFile(String imageTilePath, int resolutionX, int resolutionY, int zoomLevel, int partitionOnX, int partitionOnY)
            throws Exception
    {
        logger.info("[Sedona-Viz][stitchImagePartitions][Start]");

        BufferedImage stitchedImage = BigBufferedImage.create(resolutionX, resolutionY, BufferedImage.TYPE_INT_ARGB);
        //Stitch all image partitions together
        for (int i = 0; i < partitionOnX * partitionOnY; i++) {
            BufferedImage imageTile = null;
            try {
                imageTile = ImageIO.read(new File("" + imageTilePath + "-" + RasterizationUtils.getImageTileName(zoomLevel, partitionOnX, partitionOnY, i) + ".png"));
            }
            catch (IOException e) {
                continue;
            }
            Tuple2<Integer, Integer> partitionCoordinate = RasterizationUtils.Decode1DTo2DId(partitionOnX, partitionOnY, i);
            int partitionMinX = partitionCoordinate._1 * Math.round(resolutionX / partitionOnX);
            int partitionMinY = partitionCoordinate._2 * Math.round(resolutionY / partitionOnY);
            //if(partitionMinX!=0){partitionMinX--;}
            //if(partitionMinY!=0){partitionMinY--;}
            int[] rgbArray = imageTile.getRGB(0, 0, imageTile.getWidth(), imageTile.getHeight(), null, 0, imageTile.getWidth());
            int partitionMaxX = partitionMinX + imageTile.getWidth();
            int partitionMaxY = partitionMinY + imageTile.getHeight();
            logger.debug("[Sedona-Viz][stitchImagePartitions] stitching image tile..." + i + " ResolutionX " + resolutionX + " ResolutionY " + resolutionY);
            logger.debug("[Sedona-Viz][stitchImagePartitions] stitching a image tile..." + i + " MinX " + partitionMinX + " MaxX " + partitionMaxX + " MinY " + partitionMinY + " MaxY " + partitionMaxY);
            stitchedImage.setRGB(partitionMinX, partitionMinY, imageTile.getWidth(), imageTile.getHeight(), rgbArray, 0, imageTile.getWidth());
        }
        ImageGenerator imageGenerator = new ImageGenerator();
        imageGenerator.SaveRasterImageAsLocalFile(stitchedImage, imageTilePath + "-" + zoomLevel + "-stitched", ImageType.PNG);
        logger.info("[Sedona-Viz][stitchImagePartitions][Stop]");
        return true;
    }

    /**
     * Stitch image partitions from S 3 file.
     *
     * @param regionName the region name
     * @param accessKey the access key
     * @param secretKey the secret key
     * @param bucketName the bucket name
     * @param imageTilePath the image tile path
     * @param resolutionX the resolution X
     * @param resolutionY the resolution Y
     * @param zoomLevel the zoom level
     * @param partitionOnX the partition on X
     * @param partitionOnY the partition on Y
     * @return true, if successful
     * @throws Exception the exception
     */
    public static boolean stitchImagePartitionsFromS3File(String regionName, String accessKey, String secretKey, String bucketName, String imageTilePath, int resolutionX, int resolutionY, int zoomLevel, int partitionOnX, int partitionOnY)
            throws Exception
    {
        logger.info("[Sedona-Viz][stitchImagePartitions][Start]");

        BufferedImage stitchedImage = BigBufferedImage.create(resolutionX, resolutionY, BufferedImage.TYPE_INT_ARGB);
        S3Operator s3Operator = new S3Operator(regionName, accessKey, secretKey);
        //Stitch all image partitions together
        for (int i = 0; i < partitionOnX * partitionOnY; i++) {
            BufferedImage imageTile = null;
            try {
                imageTile = s3Operator.getImage(bucketName, imageTilePath + "-" + RasterizationUtils.getImageTileName(zoomLevel, partitionOnX, partitionOnY, i) + ".png");
            }
            catch (AmazonS3Exception e) {
                continue;
            }
            Tuple2<Integer, Integer> partitionCoordinate = RasterizationUtils.Decode1DTo2DId(partitionOnX, partitionOnY, i);
            int partitionMinX = partitionCoordinate._1 * Math.round(resolutionX / partitionOnX);
            int partitionMinY = partitionCoordinate._2 * Math.round(resolutionY / partitionOnY);
            //if(partitionMinX!=0){partitionMinX--;}
            //if(partitionMinY!=0){partitionMinY--;}
            int[] rgbArray = imageTile.getRGB(0, 0, imageTile.getWidth(), imageTile.getHeight(), null, 0, imageTile.getWidth());
            int partitionMaxX = partitionMinX + imageTile.getWidth();
            int partitionMaxY = partitionMinY + imageTile.getHeight();
            logger.debug("[Sedona-Viz][stitchImagePartitions] stitching image tile..." + i + " ResolutionX " + resolutionX + " ResolutionY " + resolutionY);
            logger.debug("[Sedona-Viz][stitchImagePartitions] stitching a image tile..." + i + " MinX " + partitionMinX + " MaxX " + partitionMaxX + " MinY " + partitionMinY + " MaxY " + partitionMaxY);
            stitchedImage.setRGB(partitionMinX, partitionMinY, imageTile.getWidth(), imageTile.getHeight(), rgbArray, 0, imageTile.getWidth());
        }
        ImageGenerator imageGenerator = new ImageGenerator();
        imageGenerator.SaveRasterImageAsS3File(stitchedImage, regionName, accessKey, secretKey, bucketName, imageTilePath + "-" + zoomLevel + "-stitched", ImageType.PNG);
        logger.info("[Sedona-Viz][stitchImagePartitions][Stop]");
        return true;
    }

    /**
     * Stitch image partitions from hadoop file.
     *
     * @param imageTilePath the image tile path
     * @param resolutionX the resolution X
     * @param resolutionY the resolution Y
     * @param zoomLevel the zoom level
     * @param partitionOnX the partition on X
     * @param partitionOnY the partition on Y
     * @return true, if successful
     * @throws Exception the exception
     */
    public static boolean stitchImagePartitionsFromHadoopFile(String imageTilePath, int resolutionX, int resolutionY, int zoomLevel, int partitionOnX, int partitionOnY)
            throws Exception
    {
        logger.info("[Sedona-Viz][stitchImagePartitions][Start]");

        BufferedImage stitchedImage = BigBufferedImage.create(resolutionX, resolutionY, BufferedImage.TYPE_INT_ARGB);

        String[] splitString = imageTilePath.split(":");
        String hostName = splitString[0] + ":" + splitString[1];
        String[] portAndPath = splitString[2].split("/");
        String port = portAndPath[0];
        String localPath = "";
        for (int i = 1; i < portAndPath.length; i++) {
            localPath += "/" + portAndPath[i];
        }

        Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        FileSystem hdfs = FileSystem.get(new URI(hostName + ":" + port), hadoopConf);

        //Stitch all image partitions together
        for (int i = 0; i < partitionOnX * partitionOnY; i++) {
            BufferedImage imageTile = null;
            try {
                if (hdfs.exists(new org.apache.hadoop.fs.Path(localPath + "-" + RasterizationUtils.getImageTileName(zoomLevel, partitionOnX, partitionOnY, i) + ".png"))) {
                    InputStream inputStream = hdfs.open(new org.apache.hadoop.fs.Path(localPath + "-" + i + ".png"));
                    imageTile = ImageIO.read(inputStream);
                    inputStream.close();
                    hdfs.close();
                }
                else {
                    continue;
                }
            }
            catch (IOException e) {
                continue;
            }
            Tuple2<Integer, Integer> partitionCoordinate = RasterizationUtils.Decode1DTo2DId(partitionOnX, partitionOnY, i);
            int partitionMinX = partitionCoordinate._1 * Math.round(resolutionX / partitionOnX);
            int partitionMinY = partitionCoordinate._2 * Math.round(resolutionY / partitionOnY);
            //if(partitionMinX!=0){partitionMinX--;}
            //if(partitionMinY!=0){partitionMinY--;}
            int[] rgbArray = imageTile.getRGB(0, 0, imageTile.getWidth(), imageTile.getHeight(), null, 0, imageTile.getWidth());
            int partitionMaxX = partitionMinX + imageTile.getWidth();
            int partitionMaxY = partitionMinY + imageTile.getHeight();
            logger.debug("[Sedona-Viz][stitchImagePartitions] stitching image tile..." + i + " ResolutionX " + resolutionX + " ResolutionY " + resolutionY);
            logger.debug("[Sedona-Viz][stitchImagePartitions] stitching a image tile..." + i + " MinX " + partitionMinX + " MaxX " + partitionMaxX + " MinY " + partitionMinY + " MaxY " + partitionMaxY);
            stitchedImage.setRGB(partitionMinX, partitionMinY, imageTile.getWidth(), imageTile.getHeight(), rgbArray, 0, imageTile.getWidth());
        }
        ImageGenerator imageGenerator = new ImageGenerator();
        imageGenerator.SaveRasterImageAsLocalFile(stitchedImage, imageTilePath + "-" + zoomLevel + "-stitched", ImageType.PNG);
        logger.info("[Sedona-Viz][stitchImagePartitions][Stop]");
        return true;
    }
}
