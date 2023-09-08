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
package org.apache.sedona.viz.extension.imageGenerator;

import org.apache.log4j.Logger;
import org.apache.sedona.viz.core.ImageGenerator;

// TODO: Auto-generated Javadoc

/**
 * The Class SedonaVizImageGenerator.
 */
public class SedonaVizImageGenerator
        extends ImageGenerator
{

    /**
     * The Constant logger.
     */
    final static Logger logger = Logger.getLogger(SedonaVizImageGenerator.class);

//    /**
//     * Save raster image as local file.
//     *
//     * @param distributedImage the distributed image
//     * @param outputPath the output path
//     * @param imageType the image type
//     * @param zoomLevel the zoom level
//     * @param partitionOnX the partition on X
//     * @param partitionOnY the partition on Y
//     * @return true, if successful
//     * @throws Exception the exception
//     */
//    public boolean SaveRasterImageAsLocalFile(JavaPairRDD<Integer, ImageSerializableWrapper> distributedImage, final String outputPath, final ImageType imageType, final int zoomLevel, final int partitionOnX, final int partitionOnY)
//            throws Exception
//    {
//        logger.info("[Sedona-VizViz][SaveRasterImageAsLocalFile][Start]");
//        for (int i = 0; i < partitionOnX * partitionOnY; i++) {
//            deleteLocalFile(outputPath + "-" + RasterizationUtils.getImageTileName(zoomLevel, partitionOnX, partitionOnY, i), imageType);
//        }
//        distributedImage.foreach(new VoidFunction<Tuple2<Integer, ImageSerializableWrapper>>()
//        {
//            @Override
//            public void call(Tuple2<Integer, ImageSerializableWrapper> integerImageSerializableWrapperTuple2)
//                    throws Exception
//            {
//                SaveRasterImageAsLocalFile(integerImageSerializableWrapperTuple2._2.getImage(), outputPath + "-" + RasterizationUtils.getImageTileName(zoomLevel, partitionOnX, partitionOnY, integerImageSerializableWrapperTuple2._1), imageType);
//            }
//        });
//        logger.info("[Sedona-VizViz][SaveRasterImageAsLocalFile][Stop]");
//        return true;
//    }
//
//    /**
//     * Save raster image as hadoop file.
//     *
//     * @param distributedImage the distributed image
//     * @param outputPath the output path
//     * @param imageType the image type
//     * @param zoomLevel the zoom level
//     * @param partitionOnX the partition on X
//     * @param partitionOnY the partition on Y
//     * @return true, if successful
//     * @throws Exception the exception
//     */
//    public boolean SaveRasterImageAsHadoopFile(JavaPairRDD<Integer, ImageSerializableWrapper> distributedImage, final String outputPath, final ImageType imageType, final int zoomLevel, final int partitionOnX, final int partitionOnY)
//            throws Exception
//    {
//        logger.info("[Sedona-VizViz][SaveRasterImageAsHadoopFile][Start]");
//        for (int i = 0; i < partitionOnX * partitionOnY; i++) {
//            deleteHadoopFile(outputPath + "-" + RasterizationUtils.getImageTileName(zoomLevel, partitionOnX, partitionOnY, i) + ".", imageType);
//        }
//        distributedImage.foreach(new VoidFunction<Tuple2<Integer, ImageSerializableWrapper>>()
//        {
//            @Override
//            public void call(Tuple2<Integer, ImageSerializableWrapper> integerImageSerializableWrapperTuple2)
//                    throws Exception
//            {
//                SaveRasterImageAsHadoopFile(integerImageSerializableWrapperTuple2._2.getImage(), outputPath + "-" + RasterizationUtils.getImageTileName(zoomLevel, partitionOnX, partitionOnY, integerImageSerializableWrapperTuple2._1), imageType);
//            }
//        });
//        logger.info("[Sedona-VizViz][SaveRasterImageAsHadoopFile][Stop]");
//        return true;
//    }
//
//    /**
//     * Save raster image as S 3 file.
//     *
//     * @param distributedImage the distributed image
//     * @param regionName the region name
//     * @param accessKey the access key
//     * @param secretKey the secret key
//     * @param bucketName the bucket name
//     * @param path the path
//     * @param imageType the image type
//     * @param zoomLevel the zoom level
//     * @param partitionOnX the partition on X
//     * @param partitionOnY the partition on Y
//     * @return true, if successful
//     */
//    public boolean SaveRasterImageAsS3File(JavaPairRDD<Integer, ImageSerializableWrapper> distributedImage,
//            final String regionName, final String accessKey, final String secretKey,
//            final String bucketName, final String path, final ImageType imageType, final int zoomLevel, final int partitionOnX, final int partitionOnY)
//    {
//        logger.info("[Sedona-VizViz][SaveRasterImageAsS3File][Start]");
//        S3Operator s3Operator = new S3Operator(regionName, accessKey, secretKey);
//        for (int i = 0; i < partitionOnX * partitionOnY; i++) {
//            s3Operator.deleteImage(bucketName, path + "-" + RasterizationUtils.getImageTileName(zoomLevel, partitionOnX, partitionOnY, i) + "." + imageType.getTypeName());
//        }
//        distributedImage.foreach(new VoidFunction<Tuple2<Integer, ImageSerializableWrapper>>()
//        {
//            @Override
//            public void call(Tuple2<Integer, ImageSerializableWrapper> integerImageSerializableWrapperTuple2)
//                    throws Exception
//            {
//                SaveRasterImageAsS3File(integerImageSerializableWrapperTuple2._2.getImage(), regionName, accessKey, secretKey, bucketName, path + "-" + RasterizationUtils.getImageTileName(zoomLevel, partitionOnX, partitionOnY, integerImageSerializableWrapperTuple2._1), imageType);
//            }
//        });
//        logger.info("[Sedona-VizViz][SaveRasterImageAsS3File][Stop]");
//        return true;
//    }
//
//    /**
//     * Save raster image as local file.
//     *
//     * @param rasterImage the raster image
//     * @param outputPath the output path
//     * @param imageType the image type
//     * @return true, if successful
//     * @throws Exception the exception
//     */
//    public boolean SaveRasterImageAsLocalFile(BufferedImage rasterImage, String outputPath, ImageType imageType)
//            throws Exception
//    {
//        logger.info("[Sedona-VizViz][SaveRasterImageAsLocalFile][Start]");
//        File outputImage = new File(outputPath + "." + imageType.getTypeName());
//        outputImage.getParentFile().mkdirs();
//        try {
//            ImageIO.write(rasterImage, imageType.getTypeName(), outputImage);
//        }
//        catch (IOException e) {
//            e.printStackTrace();
//        }
//        logger.info("[Sedona-VizViz][SaveRasterImageAsLocalFile][Stop]");
//        return true;
//    }
//
//    /**
//     * Save raster image as hadoop file.
//     *
//     * @param rasterImage the raster image
//     * @param originalOutputPath the original output path
//     * @param imageType the image type
//     * @return true, if successful
//     * @throws Exception the exception
//     */
//    public boolean SaveRasterImageAsHadoopFile(BufferedImage rasterImage, String originalOutputPath, ImageType imageType)
//            throws Exception
//    {
//        logger.info("[Sedona-VizViz][SaveRasterImageAsHadoopFile][Start]");
//        // Locate HDFS path
//        String outputPath = originalOutputPath + "." + imageType.getTypeName();
//        String[] splitString = outputPath.split(":");
//        String hostName = splitString[0] + ":" + splitString[1];
//        String[] portAndPath = splitString[2].split("/");
//        String port = portAndPath[0];
//        String localPath = "";
//        for (int i = 1; i < portAndPath.length; i++) {
//            localPath += "/" + portAndPath[i];
//        }
//        localPath += "." + imageType.getTypeName();
//        // Delete existing files
//        Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
//        logger.info("[Sedona-VizViz][SaveRasterImageAsSparkFile] HDFS URI BASE: " + hostName + ":" + port);
//        FileSystem hdfs = FileSystem.get(new URI(hostName + ":" + port), hadoopConf);
//        logger.info("[Sedona-VizViz][SaveRasterImageAsSparkFile] Check the existence of path: " + localPath);
//        if (hdfs.exists(new org.apache.hadoop.fs.Path(localPath))) {
//            logger.info("[Sedona-VizViz][SaveRasterImageAsSparkFile] Deleting path: " + localPath);
//            hdfs.delete(new org.apache.hadoop.fs.Path(localPath), true);
//            logger.info("[Sedona-VizViz][SaveRasterImageAsSparkFile] Deleted path: " + localPath);
//        }
//        Path path = new Path(outputPath);
//        FSDataOutputStream out = hdfs.create(path);
//        ImageIO.write(rasterImage, "png", out);
//        out.close();
//        hdfs.close();
//        logger.info("[Sedona-VizViz][SaveRasterImageAsHadoopFile][Stop]");
//        return true;
//    }
//
//    /**
//     * Save raster image as S 3 file.
//     *
//     * @param rasterImage the raster image
//     * @param regionName the region name
//     * @param accessKey the access key
//     * @param secretKey the secret key
//     * @param bucketName the bucket name
//     * @param path the path
//     * @param imageType the image type
//     * @return true, if successful
//     * @throws IOException Signals that an I/O exception has occurred.
//     */
//    public boolean SaveRasterImageAsS3File(BufferedImage rasterImage, String regionName, String accessKey, String secretKey, String bucketName, String path, ImageType imageType)
//            throws IOException
//    {
//        logger.info("[Sedona-VizViz][SaveRasterImageAsS3File][Start]");
//        S3Operator s3Operator = new S3Operator(regionName, accessKey, secretKey);
//        s3Operator.putImage(bucketName, path + "." + imageType.getTypeName(), rasterImage);
//        logger.info("[Sedona-VizViz][SaveRasterImageAsS3File][Stop]");
//        return true;
//    }
//
//    /**
//     * Save vector image as local file.
//     *
//     * @param distributedImage the distributed image
//     * @param outputPath the output path
//     * @param imageType the image type
//     * @return true, if successful
//     * @throws Exception the exception
//     */
//    public boolean SaveVectorImageAsLocalFile(JavaPairRDD<Integer, String> distributedImage, String outputPath, ImageType imageType)
//            throws Exception
//    {
//        logger.info("[Sedona-VizViz][SaveVectormageAsLocalFile][Start]");
//        JavaRDD<String> distributedVectorImageNoKey = distributedImage.map(new Function<Tuple2<Integer, String>, String>()
//        {
//
//            @Override
//            public String call(Tuple2<Integer, String> vectorObject)
//                    throws Exception
//            {
//                return vectorObject._2();
//            }
//        });
//        this.SaveVectorImageAsLocalFile(distributedVectorImageNoKey.collect(), outputPath, imageType);
//        logger.info("[Sedona-VizViz][SaveVectormageAsLocalFile][Stop]");
//        return true;
//    }
//
//    /**
//     * Save vector image as local file.
//     *
//     * @param vectorImage the vector image
//     * @param outputPath the output path
//     * @param imageType the image type
//     * @return true, if successful
//     * @throws Exception the exception
//     */
//    public boolean SaveVectorImageAsLocalFile(List<String> vectorImage, String outputPath, ImageType imageType)
//            throws Exception
//    {
//        logger.info("[Sedona-VizViz][SaveVectorImageAsLocalFile][Start]");
//        File outputImage = new File(outputPath + "." + imageType.getTypeName());
//        outputImage.getParentFile().mkdirs();
//
//        BufferedWriter bw = null;
//        FileWriter fw = null;
//        try {
//            fw = new FileWriter(outputImage);
//            bw = new BufferedWriter(fw);
//            for (String svgElement : vectorImage) {
//                bw.write(svgElement);
//            }
//        }
//        catch (IOException e) {
//
//            e.printStackTrace();
//        }
//        finally {
//
//            try {
//
//                if (bw != null) { bw.close(); }
//
//                if (fw != null) { fw.close(); }
//            }
//            catch (IOException ex) {
//
//                ex.printStackTrace();
//            }
//        }
//        logger.info("[Sedona-VizViz][SaveVectorImageAsLocalFile][Stop]");
//        return true;
//    }
//
//    /**
//     * Delete hadoop file.
//     *
//     * @param originalOutputPath the original output path
//     * @param imageType the image type
//     * @return true, if successful
//     * @throws Exception the exception
//     */
//    public boolean deleteHadoopFile(String originalOutputPath, ImageType imageType)
//            throws Exception
//    {
//        String outputPath = originalOutputPath + "." + imageType.getTypeName();
//        String[] splitString = outputPath.split(":");
//        String hostName = splitString[0] + ":" + splitString[1];
//        String[] portAndPath = splitString[2].split("/");
//        String port = portAndPath[0];
//        String localPath = "";
//        for (int i = 1; i < portAndPath.length; i++) {
//            localPath += "/" + portAndPath[i];
//        }
//        localPath += "." + imageType.getTypeName();
//        // Delete existing files
//        Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
//        logger.info("[Sedona-VizViz][SaveRasterImageAsSparkFile] HDFS URI BASE: " + hostName + ":" + port);
//        FileSystem hdfs = FileSystem.get(new URI(hostName + ":" + port), hadoopConf);
//        logger.info("[Sedona-VizViz][SaveRasterImageAsSparkFile] Check the existence of path: " + localPath);
//        if (hdfs.exists(new org.apache.hadoop.fs.Path(localPath))) {
//            logger.info("[Sedona-VizViz][SaveRasterImageAsSparkFile] Deleting path: " + localPath);
//            hdfs.delete(new org.apache.hadoop.fs.Path(localPath), true);
//            logger.info("[Sedona-VizViz][SaveRasterImageAsSparkFile] Deleted path: " + localPath);
//        }
//        return true;
//    }
//
//    /**
//     * Delete local file.
//     *
//     * @param originalOutputPath the original output path
//     * @param imageType the image type
//     * @return true, if successful
//     */
//    public boolean deleteLocalFile(String originalOutputPath, ImageType imageType)
//    {
//        File file = null;
//        try {
//
//            // create new file
//            file = new File(originalOutputPath + "." + imageType.getTypeName());
//
//            // tries to delete a non-existing file
//            file.delete();
//        }
//        catch (Exception e) {
//
//            // if any error occurs
//            e.printStackTrace();
//        }
//        return true;
//    }
//
//    /**
//     * Save raster image as local file.
//     *
//     * @param distributedImage the distributed image
//     * @param outputPath the output path
//     * @param imageType the image type
//     * @return true, if successful
//     * @throws Exception the exception
//     */
//    public boolean SaveRasterImageAsLocalFile(JavaPairRDD<Integer, ImageSerializableWrapper> distributedImage, String outputPath, ImageType imageType)
//            throws Exception
//    {
//        List<Tuple2<Integer, ImageSerializableWrapper>> imagePartitions = distributedImage.collect();
//        for (Tuple2<Integer, ImageSerializableWrapper> imagePartition : imagePartitions) {
//            SaveRasterImageAsLocalFile(imagePartition._2.getImage(), outputPath + "-" + imagePartition._1, imageType);
//        }
//        return true;
//    }
}
