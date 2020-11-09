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
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.sedona.viz.utils.ColorizeOption;
import org.apache.sedona.viz.utils.Pixel;
import org.apache.sedona.viz.utils.RasterizationUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import scala.Tuple2;

import java.awt.Color;
import java.awt.Graphics;
import java.awt.image.BufferedImage;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

// TODO: Auto-generated Javadoc
class RasterPixelCountComparator
        implements Comparator<Tuple2<Pixel, Double>>, Serializable
{

    @Override
    public int compare(Tuple2<Pixel, Double> spatialObject1, Tuple2<Pixel, Double> spatialObject2)
    {
        if (spatialObject1._2 > spatialObject2._2) {
            return 1;
        }
        else if (spatialObject1._2 < spatialObject2._2) {
            return -1;
        }
        else { return 0; }
    }
}

class VectorObjectCountComparator
        implements Comparator<Tuple2<Object, Double>>, Serializable
{

    @Override
    public int compare(Tuple2<Object, Double> spatialObject1, Tuple2<Object, Double> spatialObject2)
    {
        if (spatialObject1._2 > spatialObject2._2) {
            return 1;
        }
        else if (spatialObject1._2 < spatialObject2._2) {
            return -1;
        }
        else { return 0; }
    }
}

class PixelSerialIdComparator
        implements Comparator<Tuple2<Pixel, Double>>
{

    @Override
    public int compare(Tuple2<Pixel, Double> spatialObject1, Tuple2<Pixel, Double> spatialObject2)
    {
        int spatialObject1id = spatialObject1._1().hashCode();
        int spatialObject2id = spatialObject2._1().hashCode();

        if (spatialObject1id > spatialObject2id) {
            return 1;
        }
        else if (spatialObject1id < spatialObject2id) {
            return -1;
        }
        else { return 0; }
    }
}

/**
 * The Class VisualizationOperator.
 */
public abstract class VisualizationOperator
        implements Serializable
{

    /**
     * The Constant logger.
     */
    final static Logger logger = Logger.getLogger(VisualizationOperator.class);
    /**
     * The raster image.
     */
    public BufferedImage rasterImage = null;
    /**
     * The distributed raster image.
     */
    public JavaPairRDD<Integer, ImageSerializableWrapper> distributedRasterImage = null;
    /**
     * The vector image.
     */
    public List<String> vectorImage = null;
    /**
     * The distributed vector image.
     */
    // This pair RDD only contains three distinct keys: 0, 1, 2. 0 is the SVG file header, 1 is SVG file body, 2 is SVG file footer.
    public JavaPairRDD<Integer, String> distributedVectorImage = null;
    /**
     * The colorize option.
     */
    protected ColorizeOption colorizeOption = ColorizeOption.NORMAL;
    /**
     * The max pixel count.
     */
    protected Double maxPixelCount = -1.0;
    /**
     * The reverse spatial coordinate.
     */
    protected boolean reverseSpatialCoordinate; // Switch the x and y when draw the final image
    /**
     * The resolution X.
     */
    protected int resolutionX;
    /**
     * The resolution Y.
     */
    protected int resolutionY;
    /**
     * The dataset boundary.
     */
    protected Envelope datasetBoundary;
    /**
     * The red.
     */
    protected int red = 255;

    /*
     * Parameters determine raster image
     */
    /**
     * The green.
     */
    protected int green = 255;
    /**
     * The blue.
     */
    protected int blue = 255;
    /**
     * The color alpha.
     */
    protected int colorAlpha = 0;
    /**
     * The control color channel.
     */
    protected Color controlColorChannel = Color.green;
    /**
     * The use inverse ratio for control color channel.
     */
    protected boolean useInverseRatioForControlColorChannel = true;
    /**
     * The distributed raster count matrix.
     */
    protected JavaPairRDD<Pixel, Double> distributedRasterCountMatrix;
    /**
     * The distributed raster color matrix.
     */
    protected JavaPairRDD<Pixel, Integer> distributedRasterColorMatrix;
    /**
     * The distributed vector objects.
     */
    /*
     * Parameters determine vector image
     */
    protected JavaPairRDD<Object, Double> distributedVectorObjects;
    /**
     * The distributed vector colors.
     */
    protected JavaPairRDD<Object, Color> distributedVectorColors;
    /**
     * The only draw outline.
     */
    protected boolean onlyDrawOutline = true;
    /**
     * The Photo filter convolution matrix.
     */
    /*
     * Parameters controls Photo Filter
     */
    protected Double[][] PhotoFilterConvolutionMatrix = null;
    /**
     * The photo filter radius.
     */
    protected int photoFilterRadius;
    /**
     * The partition X.
     */
    /*
     * Parameter controls spatial partitioning
     */
    protected int partitionX;
    /**
     * The partition Y.
     */
    protected int partitionY;
    /**
     * The partition interval X.
     */
    protected int partitionIntervalX;
    /**
     * The partition interval Y.
     */
    protected int partitionIntervalY;

    /*
     * Parameter tells whether do photo filter in parallel and do rendering in parallel
     */
    /**
     * The has been spatial partitioned.
     */
    protected boolean hasBeenSpatialPartitioned = false;
    /**
     * The parallel photo filter.
     */
    protected boolean parallelPhotoFilter = false;

    /*
     * Parameter for the overall system
     */
    /**
     * The parallel render image.
     */
    protected boolean parallelRenderImage = false;

    /**
     * Instantiates a new visualization operator.
     *
     * @param resolutionX the resolution X
     * @param resolutionY the resolution Y
     * @param datasetBoundary the dataset boundary
     * @param colorizeOption the colorize option
     * @param reverseSpatialCoordinate the reverse spatial coordinate
     * @param partitionX the partition X
     * @param partitionY the partition Y
     * @param parallelPhotoFilter the parallel photo filter
     * @param parallelRenderImage the parallel render image
     * @param generateVectorImage the generate vector image
     */
    public VisualizationOperator(int resolutionX, int resolutionY, Envelope datasetBoundary, ColorizeOption colorizeOption, boolean reverseSpatialCoordinate,
            int partitionX, int partitionY, boolean parallelPhotoFilter, boolean parallelRenderImage, boolean generateVectorImage)
    {
        logger.info("[Sedona-VizViz][Constructor][Start]");
        this.resolutionX = resolutionX;
        this.resolutionY = resolutionY;
        this.datasetBoundary = datasetBoundary;
        this.reverseSpatialCoordinate = reverseSpatialCoordinate;
        this.parallelRenderImage = parallelRenderImage;
        /*
         * Variables below control how to initialize a raster image
         */
        this.colorizeOption = colorizeOption;
        this.partitionX = partitionX;
        this.partitionY = partitionY;
        this.partitionIntervalX = this.resolutionX / this.partitionX;
        this.partitionIntervalY = this.resolutionY / this.partitionY;
        this.parallelPhotoFilter = parallelPhotoFilter;
        logger.info("[Sedona-VizViz][Constructor][Stop]");
    }

    /**
     * Inits the photo filter weight matrix.
     *
     * @param photoFilter the photo filter
     * @return true, if successful
     */
    protected boolean InitPhotoFilterWeightMatrix(PhotoFilter photoFilter)
    {
        this.photoFilterRadius = photoFilter.getFilterRadius();
        this.PhotoFilterConvolutionMatrix = photoFilter.getConvolutionMatrix();
        return true;
    }

    /**
     * Spatial partitioning without duplicates.
     *
     * @return true, if successful
     * @throws Exception the exception
     */
    private boolean spatialPartitioningWithoutDuplicates()
            throws Exception
    {
        this.distributedRasterColorMatrix = this.distributedRasterColorMatrix.mapToPair(new PairFunction<Tuple2<Pixel, Integer>, Pixel, Integer>()
        {
            @Override
            public Tuple2<Pixel, Integer> call(Tuple2<Pixel, Integer> pixelDoubleTuple2)
                    throws Exception
            {
                Pixel newPixel = new Pixel(pixelDoubleTuple2._1().getX(), pixelDoubleTuple2._1().getY(), resolutionX, resolutionY);
                newPixel.setDuplicate(false);
                newPixel.setCurrentPartitionId(VisualizationPartitioner.CalculatePartitionId(resolutionX, resolutionY, partitionX, partitionY, (int) pixelDoubleTuple2._1.getX(), (int) pixelDoubleTuple2._1.getY()));
                Tuple2<Pixel, Integer> newPixelDoubleTuple2 = new Tuple2<Pixel, Integer>(newPixel, pixelDoubleTuple2._2());
                return newPixelDoubleTuple2;
            }
        });
        this.distributedRasterColorMatrix = this.distributedRasterColorMatrix.partitionBy(new VisualizationPartitioner(this.resolutionX, this.resolutionY, this.partitionX, this.partitionY));
        return true;
    }

    /**
     * Spatial partitioning with duplicates.
     *
     * @return true, if successful
     * @throws Exception the exception
     */
    private boolean spatialPartitioningWithDuplicates()
            throws Exception
    {
        this.distributedRasterCountMatrix = this.distributedRasterCountMatrix.flatMapToPair(new PairFlatMapFunction<Tuple2<Pixel, Double>, Pixel, Double>()
        {
            @Override
            public Iterator<Tuple2<Pixel, Double>> call(Tuple2<Pixel, Double> pixelDoubleTuple2)
                    throws Exception
            {
                VisualizationPartitioner vizPartitioner = new VisualizationPartitioner(resolutionX, resolutionY, partitionX, partitionY);
                return vizPartitioner.assignPartitionIDs(pixelDoubleTuple2, photoFilterRadius).iterator();
            }
        });
        this.distributedRasterCountMatrix = this.distributedRasterCountMatrix.partitionBy(new VisualizationPartitioner(this.resolutionX, this.resolutionY, this.partitionX, this.partitionY));
        return true;
    }

    /**
     * Apply photo filter.
     *
     * @param sparkContext the spark context
     * @return the java pair RDD
     * @throws Exception the exception
     */
    protected JavaPairRDD<Pixel, Double> ApplyPhotoFilter(JavaSparkContext sparkContext)
            throws Exception
    {
        logger.info("[Sedona-VizViz][ApplyPhotoFilter][Start]");
        if (this.parallelPhotoFilter) {
            if (this.hasBeenSpatialPartitioned == false) {
                this.spatialPartitioningWithDuplicates();
                this.hasBeenSpatialPartitioned = true;
            }

            this.distributedRasterCountMatrix = this.distributedRasterCountMatrix.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Pixel, Double>>, Pixel, Double>()
            {

                @Override
                public Iterator<Tuple2<Pixel, Double>> call(Iterator<Tuple2<Pixel, Double>> currentPartition)
                        throws Exception
                {
                    // This function uses an efficient algorithm to recompute the pixel value. For each existing pixel,
                    // we calculate its impact for all pixels within its impact range and add its impact values.
                    HashMap<Pixel, Double> pixelCountHashMap = new HashMap<Pixel, Double>();
                    while (currentPartition.hasNext()) {
                        Tuple2<Pixel, Double> currentPixelCount = currentPartition.next();
                        Tuple2<Integer, Integer> centerPixelCoordinate = new Tuple2<Integer, Integer>((int) currentPixelCount._1().getX(), (int) currentPixelCount._1().getY());
                        if (centerPixelCoordinate._1() < 0 || centerPixelCoordinate._1() >= resolutionX || centerPixelCoordinate._2() < 0 || centerPixelCoordinate._2() >= resolutionY) {
                            // This center pixel is out of boundary so that we don't record its sum. We don't plot this pixel on the final sub image.
                            continue;
                        }
                        // Find all pixels that are in the working pixel's impact range
                        for (int x = -photoFilterRadius; x <= photoFilterRadius; x++) {
                            for (int y = -photoFilterRadius; y <= photoFilterRadius; y++) {
                                int neighborPixelX = centerPixelCoordinate._1 + x;
                                int neighborPixelY = centerPixelCoordinate._2 + y;
                                if (currentPixelCount._1().getCurrentPartitionId() < 0) {
                                    throw new Exception("[VisualizationOperator][ApplyPhotoFilter] this pixel doesn't have currentPartitionId that is assigned in VisualizationPartitioner.");
                                }
                                if (neighborPixelX < 0 || neighborPixelX >= resolutionX || neighborPixelY < 0 || neighborPixelY >= resolutionY) {
                                    // This neighbor pixel is out of boundary so that we don't record its sum. We don't plot this pixel on the final sub image.
                                    continue;
                                }
                                if (VisualizationPartitioner.CalculatePartitionId(resolutionX, resolutionY, partitionX, partitionY, neighborPixelX, neighborPixelY) != currentPixelCount._1().getCurrentPartitionId()) {
                                    // This neighbor pixel is not in this image partition so that we don't record its sum. We don't plot this pixel on the final sub image.
                                    continue;
                                }
                                Double neighborPixelCount = pixelCountHashMap.get(new Pixel(neighborPixelX, neighborPixelY, resolutionX, resolutionY));
                                // For that pixel, sum up its new count
                                if (neighborPixelCount != null) {
                                    neighborPixelCount += currentPixelCount._2() * PhotoFilterConvolutionMatrix[x + photoFilterRadius][y + photoFilterRadius];
                                    pixelCountHashMap.remove(new Pixel(neighborPixelX, neighborPixelY, resolutionX, resolutionY));
                                    Pixel newPixel = new Pixel(neighborPixelX, neighborPixelY, resolutionX, resolutionY, false, VisualizationPartitioner.CalculatePartitionId(resolutionX, resolutionY, partitionX, partitionY, neighborPixelX, neighborPixelY));
                                    pixelCountHashMap.put(newPixel, neighborPixelCount);
                                }
                                else {
                                    neighborPixelCount = currentPixelCount._2() * PhotoFilterConvolutionMatrix[x + photoFilterRadius][y + photoFilterRadius];
                                    Pixel newPixel = new Pixel(neighborPixelX, neighborPixelY, resolutionX, resolutionY, false, VisualizationPartitioner.CalculatePartitionId(resolutionX, resolutionY, partitionX, partitionY, neighborPixelX, neighborPixelY));
                                    pixelCountHashMap.put(newPixel, neighborPixelCount);
                                }
                            }
                        }
                    }
                    // Loop over the result map and convert the map. This is not efficient and can be replaced by a better way.
                    List<Tuple2<Pixel, Double>> resultSet = new ArrayList<Tuple2<Pixel, Double>>();
                    Iterator<java.util.Map.Entry<Pixel, Double>> hashmapIterator = pixelCountHashMap.entrySet().iterator();
                    while (hashmapIterator.hasNext()) {
                        Map.Entry<Pixel, Double> cursorEntry = hashmapIterator.next();
                        resultSet.add(new Tuple2<Pixel, Double>(cursorEntry.getKey(), cursorEntry.getValue()));
                    }
                    return resultSet.iterator();
                }
            });
        }
        else {
            /*
            List<Tuple2<Pixel,Double>> collectedCountMatrix = this.distributedRasterCountMatrix.collect();
            HashMap<Pixel,Double> hashCountMatrix = new HashMap<Pixel,Double>();
            for(Tuple2<Pixel,Double> pixelCount:collectedCountMatrix)
            {
                hashCountMatrix.put(pixelCount._1(),pixelCount._2());
            }
            //List<Tuple2<Pixel,Double>> originalCountMatrix = new ArrayList<Tuple2<Pixel,Double>>(collectedCountMatrix);
            //Collections.sort(originalCountMatrix, new PixelSerialIdComparator());
            final Broadcast<HashMap<Pixel,Double>> broadcastCountMatrix = sparkContext.broadcast(hashCountMatrix);
            this.distributedRasterCountMatrix = this.distributedRasterCountMatrix.mapToPair(new PairFunction<Tuple2<Pixel,Double>,Pixel,Double>()
            {
                @Override
                public Tuple2<Pixel, Double> call(Tuple2<Pixel, Double> pixel) throws Exception {
                    Tuple2<Integer,Integer> centerPixelCoordinate = new Tuple2<Integer, Integer>(pixel._1().getX(),pixel._1().getY());
                    Double pixelCount = new Double(0.0);
                    for (int x = -photoFilterRadius; x <= photoFilterRadius; x++) {
                        for (int y = -photoFilterRadius; y <= photoFilterRadius; y++) {
                            int neighborPixelX = centerPixelCoordinate._1+x;
                            int neighborPixelY = centerPixelCoordinate._2+y;
                            if(neighborPixelX<resolutionX&&neighborPixelX>=0&&neighborPixelY<resolutionY&&neighborPixelY>=0)
                            {
                                Double neighborPixelCount = broadcastCountMatrix.getValue().get(new Pixel(neighborPixelX,neighborPixelY,resolutionX,resolutionY));
                                if (neighborPixelCount==null)
                                {
                                    // Some of its neighbors are blank.
                                    neighborPixelCount=0.0;
                                }
                                pixelCount+=neighborPixelCount*PhotoFilterConvolutionMatrix[x + photoFilterRadius][y + photoFilterRadius];
                            }
                        }
                    }
                    return new Tuple2<Pixel,Double>(pixel._1,pixelCount);
                }
            });
            */
            // Spread the impact of this pixel
            this.distributedRasterCountMatrix = this.distributedRasterCountMatrix.flatMapToPair(new PairFlatMapFunction<Tuple2<Pixel, Double>, Pixel, Double>()
            {
                @Override
                public Iterator<Tuple2<Pixel, Double>> call(Tuple2<Pixel, Double> pixelCount)
                        throws Exception
                {
                    Tuple2<Integer, Integer> centerPixelCoordinate = new Tuple2<Integer, Integer>((int) pixelCount._1().getX(), (int) pixelCount._1().getY());
                    List<Tuple2<Pixel, Double>> result = new ArrayList<Tuple2<Pixel, Double>>();
                    for (int x = -photoFilterRadius; x <= photoFilterRadius; x++) {
                        for (int y = -photoFilterRadius; y <= photoFilterRadius; y++) {
                            int neighborPixelX = centerPixelCoordinate._1 + x;
                            int neighborPixelY = centerPixelCoordinate._2 + y;
                            Double pixelCountValue = 0.0;
                            if (neighborPixelX >= resolutionX || neighborPixelX < 0 || neighborPixelY >= resolutionY || neighborPixelY < 0) {
                                continue;
                            }
                            pixelCountValue = pixelCount._2() * PhotoFilterConvolutionMatrix[x + photoFilterRadius][y + photoFilterRadius];
                            result.add(new Tuple2<Pixel, Double>(new Pixel(neighborPixelX, neighborPixelY, resolutionX, resolutionY), pixelCountValue));
                        }
                    }
                    return result.iterator();
                }
            });
            this.distributedRasterCountMatrix = this.distributedRasterCountMatrix.reduceByKey(new Function2<Double, Double, Double>()
            {
                @Override
                public Double call(Double count1, Double count2)
                        throws Exception
                {
                    return count1 + count2;
                }
            });
        }
        logger.info("[Sedona-VizViz][ApplyPhotoFilter][Stop]");
        return this.distributedRasterCountMatrix;
    }

    /**
     * Colorize.
     *
     * @return true, if successful
     */
    protected boolean Colorize()
    {
        logger.info("[Sedona-VizViz][Colorize][Start]");
        if (this.maxPixelCount < 0) {
            this.maxPixelCount = this.distributedRasterCountMatrix.max(new RasterPixelCountComparator())._2;
        }
        final Double maxWeight = this.maxPixelCount;
        logger.info("[Sedona-VizViz][Colorize]maxCount is " + maxWeight);
        final Double minWeight = 0.0;
        this.distributedRasterColorMatrix = this.distributedRasterCountMatrix.mapValues(new Function<Double, Integer>()
        {

            @Override
            public Integer call(Double pixelCount)
                    throws Exception
            {
                Double currentPixelCount = pixelCount;
                if (currentPixelCount > maxWeight) {
                    currentPixelCount = maxWeight;
                }
                Double normalizedPixelCount = (currentPixelCount - minWeight) * 255 / (maxWeight - minWeight);
                Integer pixelColor = EncodeToRGB(normalizedPixelCount.intValue());
                return pixelColor;
            }
        });
        //logger.debug("[Sedona-VizViz][Colorize]output count "+this.distributedRasterColorMatrix.count());
        logger.info("[Sedona-VizViz][Colorize][Stop]");
        return true;
    }

    /**
     * Render image.
     *
     * @param sparkContext the spark context
     * @return true, if successful
     * @throws Exception the exception
     */
    protected boolean RenderImage(JavaSparkContext sparkContext)
            throws Exception
    {
        logger.info("[Sedona-VizViz][RenderImage][Start]");
        if (this.parallelRenderImage == true) {
            if (this.hasBeenSpatialPartitioned == false) {
                this.spatialPartitioningWithoutDuplicates();
                this.hasBeenSpatialPartitioned = true;
            }
            this.distributedRasterImage = this.distributedRasterColorMatrix.mapPartitionsToPair(
                    new PairFlatMapFunction<Iterator<Tuple2<Pixel, Integer>>, Integer, ImageSerializableWrapper>()
                    {
                        @Override
                        public Iterator<Tuple2<Integer, ImageSerializableWrapper>> call(Iterator<Tuple2<Pixel, Integer>> currentPartition)
                                throws Exception
                        {
                            BufferedImage imagePartition = new BufferedImage(partitionIntervalX, partitionIntervalY, BufferedImage.TYPE_INT_ARGB);
                            Tuple2<Pixel, Integer> pixelColor = null;
                            while (currentPartition.hasNext()) {
                                //Render color in this image partition pixel-wise.
                                pixelColor = currentPartition.next();

                                if (pixelColor._1().getX() < 0 || pixelColor._1().getX() >= resolutionX || pixelColor._1().getY() < 0 || pixelColor._1().getY() >= resolutionY) {
                                    pixelColor = null;
                                    continue;
                                }
                                imagePartition.setRGB((int) pixelColor._1().getX() % partitionIntervalX, (partitionIntervalY - 1) - (int) pixelColor._1().getY() % partitionIntervalY, pixelColor._2);
                            }
                            List<Tuple2<Integer, ImageSerializableWrapper>> result = new ArrayList<Tuple2<Integer, ImageSerializableWrapper>>();
                            if (pixelColor == null) {
                                // No pixels in this partition. Skip this subimage
                                return result.iterator();
                            }
                            logger.info("[Sedona-VizViz][Render]add a image partition into result set " + pixelColor._1().getCurrentPartitionId());
                            result.add(new Tuple2<Integer, ImageSerializableWrapper>(pixelColor._1().getCurrentPartitionId(), new ImageSerializableWrapper(imagePartition)));
                            return result.iterator();
                        }
                    });
            //logger.debug("[Sedona-VizViz][Render]output count "+this.distributedRasterImage.count());
        }
        else if (this.parallelRenderImage == false) {
            // Draw full size image in parallel
            this.distributedRasterImage = this.distributedRasterColorMatrix.mapPartitionsToPair(
                    new PairFlatMapFunction<Iterator<Tuple2<Pixel, Integer>>, Integer, ImageSerializableWrapper>()
                    {
                        @Override
                        public Iterator<Tuple2<Integer, ImageSerializableWrapper>> call(Iterator<Tuple2<Pixel, Integer>> currentPartition)
                                throws Exception
                        {
                            BufferedImage imagePartition = new BufferedImage(resolutionX, resolutionY, BufferedImage.TYPE_INT_ARGB);
                            Tuple2<Pixel, Integer> pixelColor = null;
                            while (currentPartition.hasNext()) {
                                //Render color in this image partition pixel-wise.
                                pixelColor = currentPartition.next();
                                if (pixelColor._1().getX() < 0 || pixelColor._1().getX() >= resolutionX || pixelColor._1().getY() < 0 || pixelColor._1().getY() >= resolutionY) {
                                    pixelColor = null;
                                    continue;
                                }
                                imagePartition.setRGB((int) pixelColor._1().getX(), (resolutionY - 1) - (int) pixelColor._1().getY(), pixelColor._2);
                            }
                            List<Tuple2<Integer, ImageSerializableWrapper>> result = new ArrayList<Tuple2<Integer, ImageSerializableWrapper>>();
                            if (pixelColor == null) {
                                // No pixels in this partition. Skip this subimage
                                return result.iterator();
                            }
                            result.add(new Tuple2<Integer, ImageSerializableWrapper>(1, new ImageSerializableWrapper(imagePartition)));
                            return result.iterator();
                        }
                    });
            // Merge images together using reduce

            this.distributedRasterImage = this.distributedRasterImage.reduceByKey(new Function2<ImageSerializableWrapper, ImageSerializableWrapper, ImageSerializableWrapper>()
            {
                @Override
                public ImageSerializableWrapper call(ImageSerializableWrapper image1, ImageSerializableWrapper image2)
                        throws Exception
                {
                    // The combined image should be a full size image
                    BufferedImage combinedImage = new BufferedImage(resolutionX, resolutionY, BufferedImage.TYPE_INT_ARGB);
                    Graphics graphics = combinedImage.getGraphics();
                    graphics.drawImage(image1.image, 0, 0, null);
                    graphics.drawImage(image2.image, 0, 0, null);
                    return new ImageSerializableWrapper(combinedImage);
                }
            });
            //logger.debug("[Sedona-VizViz][RenderImage]Merged all images into one image. Result size should be 1. Actual size is "+this.distributedRasterImage.count());
            List<Tuple2<Integer, ImageSerializableWrapper>> imageList = this.distributedRasterImage.collect();
            this.rasterImage = imageList.get(0)._2().image;
			/*
			BufferedImage renderedImage = new BufferedImage(this.resolutionX,this.resolutionY,BufferedImage.TYPE_INT_ARGB);
			List<Tuple2<Pixel,Integer>> colorMatrix = this.distributedRasterColorMatrix.collect();
			for(Tuple2<Pixel,Integer> pixelColor:colorMatrix)
			{
				int pixelX = pixelColor._1().getX();
				int pixelY = pixelColor._1().getY();
				renderedImage.setRGB(pixelX, (this.resolutionY-1)-pixelY, pixelColor._2);
			}
			this.rasterImage = renderedImage;
			*/
        }
        logger.info("[Sedona-VizViz][RenderImage][Stop]");
        return true;
    }

    /**
     * Sets the max pixel count.
     *
     * @param maxPixelCount the max pixel count
     * @return true, if successful
     */
    /*
    public boolean stitchImagePartitions() throws Exception
    {
        logger.info("[Sedona-VizViz][stitchImagePartitions][Start]");
        if(this.distributedRasterImage==null)
        {
            throw new Exception("[Sedona-VizViz][stitchImagePartitions] The distributed pixel image is null. No need to stitch.");
        }
        List<Tuple2<Integer, ImageSerializableWrapper>> imagePartitions = this.distributedRasterImage.collect();
        BufferedImage renderedImage = new BufferedImage(this.resolutionX,this.resolutionY,BufferedImage.TYPE_INT_ARGB);
        //Stitch all image partitions together
        for(int i=0;i<imagePartitions.size();i++)
        {
            BufferedImage imagePartition = imagePartitions.get(i)._2.image;
            Tuple2<Integer,Integer> partitionCoordinate = RasterizationUtils.Decode1DTo2DId(this.partitionX, this.partitionY, imagePartitions.get(i)._1);
            int partitionMinX = partitionCoordinate._1*this.partitionIntervalX;
            int partitionMinY = ((this.partitionY-1)-partitionCoordinate._2)*this.partitionIntervalY;
            int[] rgbArray = imagePartition.getRGB(0, 0, imagePartition.getWidth(), imagePartition.getHeight(), null, 0, imagePartition.getWidth());
            renderedImage.setRGB(partitionMinX, partitionMinY, partitionIntervalX, partitionIntervalY, rgbArray, 0, partitionIntervalX);
        }
        this.rasterImage = renderedImage;
        logger.info("[Sedona-VizViz][stitchImagePartitions][Stop]");
        return true;
    }
    */

    /**
     * Sets the max pixel count.
     *
     * @param maxPixelCount the max pixel count
     * @return true, if successful
     */
    public boolean setMaxPixelCount(Double maxPixelCount)
    {
        this.maxPixelCount = maxPixelCount;
        return true;
    }

    /**
     * Customize color.
     *
     * @param red the red
     * @param green the green
     * @param blue the blue
     * @param colorAlpha the color alpha
     * @param controlColorChannel the control color channel
     * @param useInverseRatioForControlColorChannel the use inverse ratio for control color channel
     * @return true, if successful
     */
    public boolean CustomizeColor(int red, int green, int blue, int colorAlpha, Color controlColorChannel, boolean useInverseRatioForControlColorChannel)
    {
        logger.info("[Sedona-VizViz][CustomizeColor][Start]");
        this.red = red;
        this.green = green;
        this.blue = blue;
        this.colorAlpha = colorAlpha;
        this.controlColorChannel = controlColorChannel;

        this.useInverseRatioForControlColorChannel = useInverseRatioForControlColorChannel;
        logger.info("[Sedona-VizViz][CustomizeColor][Stop]");
        return true;
    }

    /**
     * Encode to color.
     *
     * @param normailizedCount the normailized count
     * @return the color
     * @throws Exception the exception
     */
    protected Color EncodeToColor(int normailizedCount)
            throws Exception
    {
        if (controlColorChannel.equals(Color.RED)) {
            red = useInverseRatioForControlColorChannel ? 255 - normailizedCount : normailizedCount;
        }
        else if (controlColorChannel.equals(Color.GREEN)) {
            green = useInverseRatioForControlColorChannel ? 255 - normailizedCount : normailizedCount;
        }
        else if (controlColorChannel.equals(Color.BLUE)) {
            blue = useInverseRatioForControlColorChannel ? 255 - normailizedCount : normailizedCount;
        }
        else { throw new Exception("[Sedona-VizViz][GenerateColor] Unsupported changing color color type. It should be in R,G,B"); }

        if (normailizedCount == 0) {
            return new Color(red, green, blue, 0);
        }
        return new Color(red, green, blue, colorAlpha);
    }

    /**
     * Encode to RGB.
     *
     * @param normailizedCount the normailized count
     * @return the integer
     * @throws Exception the exception
     */
    protected Integer EncodeToRGB(int normailizedCount)
            throws Exception
    {
        if (controlColorChannel.equals(Color.RED)) {
            red = useInverseRatioForControlColorChannel ? 255 - normailizedCount : normailizedCount;
        }
        else if (controlColorChannel.equals(Color.GREEN)) {
            green = useInverseRatioForControlColorChannel ? 255 - normailizedCount : normailizedCount;
        }
        else if (controlColorChannel.equals(Color.BLUE)) {
            blue = useInverseRatioForControlColorChannel ? 255 - normailizedCount : normailizedCount;
        }
        else { throw new Exception("[Sedona-VizViz][GenerateColor] Unsupported changing color color type. It should be in R,G,B"); }

        if (normailizedCount == 0) {
            return new Color(red, green, blue, 0).getRGB();
        }
        return new Color(red, green, blue, colorAlpha).getRGB();
    }

    /**
     * Rasterize.
     *
     * @param sparkContext the spark context
     * @param spatialRDD the spatial RDD
     * @param useSparkDefaultPartition the use spark default partition
     * @return the java pair RDD
     */
    protected JavaPairRDD<Pixel, Double> Rasterize(JavaSparkContext sparkContext,
            SpatialRDD spatialRDD, boolean useSparkDefaultPartition)
    {
        logger.info("[Sedona-VizViz][Rasterize][Start]");
        JavaRDD<Object> rawSpatialRDD = spatialRDD.rawSpatialRDD;
        JavaPairRDD<Pixel, Double> spatialRDDwithPixelId = rawSpatialRDD.flatMapToPair(new PairFlatMapFunction<Object, Pixel, Double>()
        {
            @Override
            public Iterator<Tuple2<Pixel, Double>> call(Object spatialObject)
                    throws Exception
            {

                if (spatialObject instanceof Point) {
                    return RasterizationUtils.FindPixelCoordinates(resolutionX, resolutionY, datasetBoundary, (Point) spatialObject, colorizeOption, reverseSpatialCoordinate).iterator();
                }
                else if (spatialObject instanceof Polygon) {
                    return RasterizationUtils.FindPixelCoordinates(resolutionX, resolutionY, datasetBoundary, (Polygon) spatialObject, reverseSpatialCoordinate).iterator();
                }
                else if (spatialObject instanceof LineString) {
                    return RasterizationUtils.FindPixelCoordinates(resolutionX, resolutionY, datasetBoundary, (LineString) spatialObject, reverseSpatialCoordinate).iterator();
                }
                else {
                    throw new Exception("[Sedona-VizViz][Rasterize] Unsupported spatial object types. Sedona-VizViz only supports Point, Polygon, LineString");
                }
            }
        });
        //JavaPairRDD<Integer, Double> originalImage = sparkContext.parallelizePairs(this.countMatrix);
        //spatialRDDwithPixelId = spatialRDDwithPixelId.union(originalImage);
        spatialRDDwithPixelId = spatialRDDwithPixelId.filter(new Function<Tuple2<Pixel, Double>, Boolean>()
        {
            @Override
            public Boolean call(Tuple2<Pixel, Double> pixelCount)
                    throws Exception
            {
                return !(pixelCount._1().getX() < 0) && !(pixelCount._1().getX() > resolutionX) && !(pixelCount._1().getY() < 0) && !(pixelCount._1().getY() > resolutionY);
            }
        });

		/*
        spatialRDDwithPixelId = spatialRDDwithPixelId.reduceByKey(new Function2<Double,Double,Double>()
		{
			@Override
			public Double call(Double count1, Double count2) throws Exception {
				if(colorizeOption==ColorizeOption.SPATIALAGGREGATION)
				{
					return count1+count2;
				}
				else
				{
					//TODO, colorizeOption for uniform color and z-axis color follow the same aggregate strategy
					// which takes the large value. We need to find a better strategy to distinguish them.
					return count1>count2?count1:count2;
				}
			}
		});
		*/
        this.distributedRasterCountMatrix = spatialRDDwithPixelId;
        //logger.debug("[Sedona-VizViz][Rasterize]output count "+this.distributedRasterCountMatrix.count());
        logger.info("[Sedona-VizViz][Rasterize][Stop]");
        return this.distributedRasterCountMatrix;
    }

    /**
     * Rasterize.
     *
     * @param sparkContext the spark context
     * @param spatialPairRDD the spatial pair RDD
     * @param useSparkDefaultPartition the use spark default partition
     * @return the java pair RDD
     */
    protected JavaPairRDD<Pixel, Double> Rasterize(JavaSparkContext sparkContext,
            JavaPairRDD<Polygon, Long> spatialPairRDD, boolean useSparkDefaultPartition)
    {
        logger.info("[Sedona-VizViz][Rasterize][Start]");
        JavaPairRDD<Pixel, Double> spatialRDDwithPixelId = spatialPairRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<Polygon, Long>, Pixel, Double>()
        {
            @Override
            public Iterator<Tuple2<Pixel, Double>> call(Tuple2<Polygon, Long> spatialObject)
                    throws Exception
            {
                return RasterizationUtils.FindPixelCoordinates(resolutionX, resolutionY, datasetBoundary, spatialObject._1(), reverseSpatialCoordinate, new Double(spatialObject._2())).iterator();
            }
        });
        //JavaPairRDD<Pixel, Double> originalImage = sparkContext.parallelizePairs(this.countMatrix);
        //JavaPairRDD<Integer,Double> completeSpatialRDDwithPixelId = spatialRDDwithPixelId.union(originalImage);

        spatialRDDwithPixelId = spatialRDDwithPixelId.filter(new Function<Tuple2<Pixel, Double>, Boolean>()
        {
            @Override
            public Boolean call(Tuple2<Pixel, Double> pixelCount)
                    throws Exception
            {
                return !(pixelCount._1().getX() < 0) && !(pixelCount._1().getX() >= resolutionX) && !(pixelCount._1().getY() < 0) && !(pixelCount._1().getY() >= resolutionY);
            }
        });

            /*
			spatialRDDwithPixelId = spatialRDDwithPixelId.reduceByKey(new Function2<Double,Double,Double>()
			{
				@Override
				public Double call(Double count1, Double count2) throws Exception {
					if(colorizeOption==ColorizeOption.SPATIALAGGREGATION)
					{
						return count1+count2;
					}
					else {
						return count1>count2?count1:count2;
					}
				}
			});
			*/

        this.distributedRasterCountMatrix = spatialRDDwithPixelId;
        //logger.debug("[Sedona-VizViz][Rasterize]output count "+this.distributedRasterCountMatrix.count());
        logger.info("[Sedona-VizViz][Rasterize][Stop]");
        return this.distributedRasterCountMatrix;
    }
}
