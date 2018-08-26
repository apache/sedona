/*
 * FILE: RasterPixelCountComparator
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
package org.datasyslab.geosparkviz.core;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;
import org.datasyslab.geosparkviz.utils.ColorizeOption;
import org.datasyslab.geosparkviz.utils.Pixel;
import org.datasyslab.geosparkviz.utils.RasterizationUtils;
import org.jfree.graphics2d.svg.SVGGraphics2D;
import scala.Tuple2;

import java.awt.Color;
import java.awt.Graphics;
import java.awt.geom.Ellipse2D;
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

    /*
     * Parameters determine raster image
     */

    /**
     * The generate vector image.
     */
    protected boolean generateVectorImage = false;
    

    /**
     * The distributed raster count matrix.
     */
    protected JavaPairRDD<Pixel, Double> distributedRasterCountMatrix;

    /**
     * The distributed raster color matrix.
     */
    protected JavaPairRDD<Pixel, Integer> distributedRasterColorMatrix;

    /**
     * The raster image.
     */
    public BufferedImage rasterImage = null;

    /**
     * The distributed raster image.
     */
    public JavaPairRDD<Integer, ImageSerializableWrapper> distributedRasterImage = null;

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
     * The vector image.
     */
    public List<String> vectorImage = null;

    /**
     * The distributed vector image.
     */
    // This pair RDD only contains three distinct keys: 0, 1, 2. 0 is the SVG file header, 1 is SVG file body, 2 is SVG file footer.
    public JavaPairRDD<Integer, String> distributedVectorImage = null;

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

    /**
     * The has been spatial partitioned.
     */
    protected boolean hasBeenSpatialPartitioned = false;

    /*
     * Parameter tells whether do photo filter in parallel and do rendering in parallel
     */
    /**
     * The parallel photo filter.
     */
    protected boolean parallelPhotoFilter = false;

    /**
     * The parallel render image.
     */
    protected boolean parallelRenderImage = false;

    /*
     * Parameter for the overall system
     */

    /**
     * The Constant logger.
     */
    final static Logger logger = Logger.getLogger(VisualizationOperator.class);

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
        logger.info("[GeoSparkViz][Constructor][Start]");
        this.resolutionX = resolutionX;
        this.resolutionY = resolutionY;
        this.datasetBoundary = datasetBoundary;
        this.reverseSpatialCoordinate = reverseSpatialCoordinate;
        this.generateVectorImage = generateVectorImage;
        this.parallelRenderImage = parallelRenderImage;

        if (this.generateVectorImage) { return; }
        /*
         * Variables below control how to initialize a raster image
         */
        this.colorizeOption = colorizeOption;
        this.partitionX = partitionX;
        this.partitionY = partitionY;
        this.partitionIntervalX = this.resolutionX / this.partitionX;
        this.partitionIntervalY = this.resolutionY / this.partitionY;
        this.parallelPhotoFilter = parallelPhotoFilter;
        logger.info("[GeoSparkViz][Constructor][Stop]");
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
                newPixel.setCurrentPartitionId(VisualizationPartitioner.CalculatePartitionId(resolutionX, resolutionY, partitionX, partitionY, pixelDoubleTuple2._1.getX(), pixelDoubleTuple2._1.getY()));
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
        logger.info("[GeoSparkViz][ApplyPhotoFilter][Start]");
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
                        Tuple2<Integer, Integer> centerPixelCoordinate = new Tuple2<Integer, Integer>(currentPixelCount._1().getX(), currentPixelCount._1().getY());
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
                    Tuple2<Integer, Integer> centerPixelCoordinate = new Tuple2<Integer, Integer>(pixelCount._1().getX(), pixelCount._1().getY());
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
        logger.info("[GeoSparkViz][ApplyPhotoFilter][Stop]");
        return this.distributedRasterCountMatrix;
    }

    /**
     * Colorize.
     *
     * @return true, if successful
     */
    protected boolean Colorize()
    {
        logger.info("[GeoSparkViz][Colorize][Start]");

        if (this.generateVectorImage) {
            if (this.maxPixelCount < 0) {
                this.maxPixelCount = this.distributedVectorObjects.max(new VectorObjectCountComparator())._2;
            }
            final Double maxWeight = this.maxPixelCount;
            final Double minWeight = 0.0;
            this.distributedVectorColors = this.distributedVectorObjects.mapValues(new Function<Double, Color>()
            {

                @Override
                public Color call(Double objectCount)
                        throws Exception
                {

                    Long normalizedObjectCount = new Double((objectCount - minWeight) * 255 / (maxWeight - minWeight)).longValue();
                    Color objectColor = EncodeToColor(normalizedObjectCount.intValue());
                    return objectColor;
                }
            });
        }
        else {
            if (this.maxPixelCount < 0) {
                this.maxPixelCount = this.distributedRasterCountMatrix.max(new RasterPixelCountComparator())._2;
            }
            final Double maxWeight = this.maxPixelCount;
            logger.info("[GeoSparkViz][Colorize]maxCount is " + maxWeight);
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
            //logger.debug("[GeoSparkViz][Colorize]output count "+this.distributedRasterColorMatrix.count());
        }

        logger.info("[GeoSparkViz][Colorize][Stop]");
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
        logger.info("[GeoSparkViz][RenderImage][Start]");
        if (this.generateVectorImage) {
            this.distributedVectorImage = this.distributedVectorColors.mapToPair(new PairFunction<Tuple2<Object, Color>, Integer, String>()
            {

                @Override
                public Tuple2<Integer, String> call(Tuple2<Object, Color> color)
                        throws Exception
                {
                    SVGGraphics2D g2 = new SVGGraphics2D(resolutionX, resolutionY);
                    if (color._1() instanceof Point) {
                        g2.setPaint(color._2());
                        Ellipse2D circle = new Ellipse2D.Double(((Point) color._1()).getCoordinate().x, ((Point) color._1()).getCoordinate().y, 1, 1);
                        g2.fill(circle);
                        String svgBody = g2.getSVGBody();
                        return new Tuple2<Integer, String>(1, svgBody);
                    }
                    else if (color._1() instanceof Polygon) {
                        g2.setPaint(color._2());
                        Coordinate[] coordinates = ((Polygon) color._1()).getCoordinates();
                        /*
                         * JTS polygon has one extra coordinate at the end. This coordinate is same with the first coordinate.
                         * But AWT.polygon does not need this extra coordinate.
                         */
                        int[] xPoints = new int[coordinates.length - 1];
                        int[] yPoints = new int[coordinates.length - 1];
                        for (int i = 0; i < coordinates.length - 1; i++) {
                            xPoints[i] = (int) coordinates[i].x;
                            yPoints[i] = (int) coordinates[i].y;
                        }
                        if (onlyDrawOutline) {
                            g2.drawPolygon(xPoints, yPoints, coordinates.length - 1);
                        }
                        else {
                            g2.fillPolygon(xPoints, yPoints, coordinates.length - 1);
                        }
                        return new Tuple2<Integer, String>(1, g2.getSVGBody());
                    }
                    else if (color._1() instanceof LineString) {
                        g2.setPaint(color._2());
                        Coordinate[] coordinates = ((LineString) color._1()).getCoordinates();
                        int[] xPoints = new int[coordinates.length];
                        int[] yPoints = new int[coordinates.length];
                        for (int i = 0; i < coordinates.length; i++) {
                            xPoints[i] = (int) coordinates[i].x;
                            yPoints[i] = (int) coordinates[i].y;
                        }
                        g2.drawPolyline(xPoints, yPoints, coordinates.length);
                        return new Tuple2<Integer, String>(1, g2.getSVGBody());
                    }
                    else {
                        throw new Exception("[GeoSparkViz][RenderImage] Unsupported spatial object types. GeoSparkViz only supports Point, Polygon, LineString");
                    }
                }
            });
            /*
             * Add SVG xml file header and footer to the distributed svg file
             */
            List<Tuple2<Integer, String>> svgHeaderFooter = new ArrayList<Tuple2<Integer, String>>();
            SVGGraphics2D g2 = new SVGGraphics2D(resolutionX, resolutionY);
            svgHeaderFooter.add(new Tuple2<Integer, String>(0, g2.getSVGHeader()));
            svgHeaderFooter.add(new Tuple2<Integer, String>(2, g2.getSVGFooter()));
            JavaPairRDD<Integer, String> distributedSVGHeaderFooter = sparkContext.parallelizePairs(svgHeaderFooter);
            this.distributedVectorImage = this.distributedVectorImage.union(distributedSVGHeaderFooter);
            this.distributedVectorImage = this.distributedVectorImage.sortByKey();
            if (this.parallelRenderImage == true) {
                return true;
            }

            JavaRDD<String> distributedVectorImageNoKey = this.distributedVectorImage.map(new Function<Tuple2<Integer, String>, String>()
            {

                @Override
                public String call(Tuple2<Integer, String> vectorObject)
                        throws Exception
                {
                    return vectorObject._2();
                }
            });
            this.vectorImage = distributedVectorImageNoKey.collect();
        }
        else if (this.parallelRenderImage == true && this.generateVectorImage == false) {
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
                                imagePartition.setRGB(pixelColor._1().getX() % partitionIntervalX, (partitionIntervalY - 1) - pixelColor._1().getY() % partitionIntervalY, pixelColor._2);
                            }
                            List<Tuple2<Integer, ImageSerializableWrapper>> result = new ArrayList<Tuple2<Integer, ImageSerializableWrapper>>();
                            if (pixelColor == null) {
                                // No pixels in this partition. Skip this subimage
                                return result.iterator();
                            }
                            logger.info("[GeoSparkViz][Render]add a image partition into result set " + pixelColor._1().getCurrentPartitionId());
                            result.add(new Tuple2<Integer, ImageSerializableWrapper>(pixelColor._1().getCurrentPartitionId(), new ImageSerializableWrapper(imagePartition)));
                            return result.iterator();
                        }
                    });
            //logger.debug("[GeoSparkViz][Render]output count "+this.distributedRasterImage.count());
        }
        else if (this.parallelRenderImage == false && this.generateVectorImage == false) {
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
                                imagePartition.setRGB(pixelColor._1().getX(), (resolutionY - 1) - pixelColor._1().getY(), pixelColor._2);
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
            //logger.debug("[GeoSparkViz][RenderImage]Merged all images into one image. Result size should be 1. Actual size is "+this.distributedRasterImage.count());
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
        logger.info("[GeoSparkViz][RenderImage][Stop]");
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
        logger.info("[GeoSparkViz][stitchImagePartitions][Start]");
        if(this.distributedRasterImage==null)
        {
            throw new Exception("[GeoSparkViz][stitchImagePartitions] The distributed pixel image is null. No need to stitch.");
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
        logger.info("[GeoSparkViz][stitchImagePartitions][Stop]");
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
        logger.info("[GeoSparkViz][CustomizeColor][Start]");
        this.red = red;
        this.green = green;
        this.blue = blue;
        this.colorAlpha = colorAlpha;
        this.controlColorChannel = controlColorChannel;

        this.useInverseRatioForControlColorChannel = useInverseRatioForControlColorChannel;
        logger.info("[GeoSparkViz][CustomizeColor][Stop]");
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
        else { throw new Exception("[GeoSparkViz][GenerateColor] Unsupported changing color color type. It should be in R,G,B"); }

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
        else { throw new Exception("[GeoSparkViz][GenerateColor] Unsupported changing color color type. It should be in R,G,B"); }

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
        logger.info("[GeoSparkViz][Rasterize][Start]");
        JavaRDD<Object> rawSpatialRDD = spatialRDD.rawSpatialRDD;
        if (generateVectorImage) {
            this.distributedVectorObjects = rawSpatialRDD.flatMapToPair(new PairFlatMapFunction<Object, Object, Double>()
            {
                @Override
                public Iterator<Tuple2<Object, Double>> call(Object spatialObject)
                        throws Exception
                {
                    GeometryFactory geometryFactory = new GeometryFactory();
                    List<Tuple2<Object, Double>> result = new ArrayList<Tuple2<Object, Double>>();
                    if (spatialObject instanceof Point) {
                        if (!datasetBoundary.covers(((Point) spatialObject).getEnvelopeInternal())) { return result.iterator(); }
                        Coordinate coordinate = RasterizationUtils.FindPixelCoordinates(resolutionX, resolutionY, datasetBoundary, ((Point) spatialObject).getCoordinate(), reverseSpatialCoordinate, false, true);
                        Point rasterizedObject = geometryFactory.createPoint(coordinate);
                        if (colorizeOption == ColorizeOption.EARTHOBSERVATION) {
                            result.add(new Tuple2<Object, Double>(rasterizedObject, ((Point) spatialObject).getCoordinate().z));
                        }
                        else {
                            result.add(new Tuple2<Object, Double>(rasterizedObject, new Double(1.0)));
                        }
                        return result.iterator();
                    }
                    else if (spatialObject instanceof Polygon) {
                        if (!datasetBoundary.covers(((Polygon) spatialObject).getEnvelopeInternal())) { return result.iterator(); }
                        Coordinate[] spatialCoordinates = RasterizationUtils.FindPixelCoordinates(resolutionX, resolutionY, datasetBoundary, ((Polygon) spatialObject).getCoordinates(), reverseSpatialCoordinate, false, true);
                        result.add(new Tuple2<Object, Double>(geometryFactory.createPolygon(spatialCoordinates), new Double(1.0)));
                        return result.iterator();
                    }
                    else if (spatialObject instanceof LineString) {
                        if (!datasetBoundary.covers(((LineString) spatialObject).getEnvelopeInternal())) { return result.iterator(); }
                        Coordinate[] spatialCoordinates = RasterizationUtils.FindPixelCoordinates(resolutionX, resolutionY, datasetBoundary, ((LineString) spatialObject).getCoordinates(), reverseSpatialCoordinate, false, true);
                        result.add(new Tuple2<Object, Double>(geometryFactory.createLineString(spatialCoordinates), new Double(1.0)));
                        return result.iterator();
                    }
                    else {
                        throw new Exception("[GeoSparkViz][Rasterize] Unsupported spatial object types. GeoSparkViz only supports Point, Polygon, LineString");
                    }
                }
            });
        }
        else {
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
                        throw new Exception("[GeoSparkViz][Rasterize] Unsupported spatial object types. GeoSparkViz only supports Point, Polygon, LineString");
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
                    if (pixelCount._1().getX() < 0 || pixelCount._1().getX() > resolutionX || pixelCount._1().getY() < 0 || pixelCount._1().getY() > resolutionY) {
                        return false;
                    }
                    else {
                        return true;
                    }
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
            //logger.debug("[GeoSparkViz][Rasterize]output count "+this.distributedRasterCountMatrix.count());
        }
        logger.info("[GeoSparkViz][Rasterize][Stop]");
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
        logger.info("[GeoSparkViz][Rasterize][Start]");
        if (generateVectorImage) {
            this.onlyDrawOutline = false;
            this.distributedVectorObjects = spatialPairRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<Polygon, Long>, Object, Double>()
            {
                @Override
                public Iterator<Tuple2<Object, Double>> call(Tuple2<Polygon, Long> spatialObject)
                        throws Exception
                {
                    GeometryFactory geometryFactory = new GeometryFactory();
                    List<Tuple2<Object, Double>> result = new ArrayList<Tuple2<Object, Double>>();
                    if (!datasetBoundary.covers(((Polygon) spatialObject._1()).getEnvelopeInternal())) { return result.iterator(); }
                    Coordinate[] spatialCoordinates = RasterizationUtils.FindPixelCoordinates(resolutionX, resolutionY, datasetBoundary, ((Polygon) spatialObject._1()).getCoordinates(), reverseSpatialCoordinate, false, true);
                    result.add(new Tuple2<Object, Double>(geometryFactory.createPolygon(spatialCoordinates), new Double(spatialObject._2())));
                    return result.iterator();
                }
            });
        }
        else {
            JavaPairRDD<Pixel, Double> spatialRDDwithPixelId = spatialPairRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<Polygon, Long>, Pixel, Double>()
            {
                @Override
                public Iterator<Tuple2<Pixel, Double>> call(Tuple2<Polygon, Long> spatialObject)
                        throws Exception
                {
                    return RasterizationUtils.FindPixelCoordinates(resolutionX, resolutionY, datasetBoundary, (Polygon) spatialObject._1(), reverseSpatialCoordinate, new Double(spatialObject._2())).iterator();
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
                    if (pixelCount._1().getX() < 0 || pixelCount._1().getX() >= resolutionX || pixelCount._1().getY() < 0 || pixelCount._1().getY() >= resolutionY) {
                        return false;
                    }
                    else {
                        return true;
                    }
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
            //logger.debug("[GeoSparkViz][Rasterize]output count "+this.distributedRasterCountMatrix.count());
        }
        logger.info("[GeoSparkViz][Rasterize][Stop]");
        return this.distributedRasterCountMatrix;
    }
}
