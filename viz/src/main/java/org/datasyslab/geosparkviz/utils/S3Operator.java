/*
 * FILE: S3Operator
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package org.datasyslab.geosparkviz.utils;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.log4j.Logger;

import javax.imageio.ImageIO;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class S3Operator
{

    private AmazonS3 s3client;

    public final static Logger logger = Logger.getLogger(S3Operator.class);

    public S3Operator(String regionName, String accessKey, String secretKey)
    {
        Regions region = Regions.fromName(regionName);
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(accessKey, secretKey);
        s3client = AmazonS3ClientBuilder.standard().withRegion(region).withCredentials(new AWSStaticCredentialsProvider(awsCreds)).build();
        logger.info("[GeoSparkViz][Constructor] Initialized a S3 client");
    }

    public boolean createBucket(String bucketName)
    {
        Bucket bucket = s3client.createBucket(bucketName);
        logger.info("[GeoSparkViz][createBucket] Created a bucket: " + bucket.toString());
        return true;
    }

    public boolean deleteImage(String bucketName, String path)
    {
        s3client.deleteObject(bucketName, path);
        logger.info("[GeoSparkViz][deleteImage] Deleted an image if exist");
        return true;
    }

    public boolean putImage(String bucketName, String path, BufferedImage rasterImage)
            throws IOException
    {
        deleteImage(bucketName, path);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ImageIO.write(rasterImage, "png", outputStream);
        byte[] buffer = outputStream.toByteArray();
        InputStream inputStream = new ByteArrayInputStream(buffer);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(buffer.length);
        s3client.putObject(new PutObjectRequest(bucketName, path, inputStream, metadata));
        inputStream.close();
        outputStream.close();
        logger.info("[GeoSparkViz][putImage] Put an image");
        return true;
    }

    public BufferedImage getImage(String bucketName, String path)
            throws Exception
    {
        logger.debug("[GeoSparkViz][getImage] Start");
        S3Object s3Object = s3client.getObject(bucketName, path);
        InputStream inputStream = s3Object.getObjectContent();
        BufferedImage rasterImage = ImageIO.read(inputStream);
        inputStream.close();
        s3Object.close();
        logger.info("[GeoSparkViz][getImage] Got an image");
        return rasterImage;
    }
}
