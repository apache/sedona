/*
 * FILE: VisualizationPartitioner
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

import org.apache.spark.Partitioner;
import org.datasyslab.geosparkviz.utils.Pixel;
import org.datasyslab.geosparkviz.utils.RasterizationUtils;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

// TODO: Auto-generated Javadoc

/**
 * The Class VisualizationPartitioner.
 */
public class VisualizationPartitioner
        extends Partitioner
        implements Serializable
{

    /**
     * The partition interval Y.
     */
    public int resolutionX, resolutionY, partitionX, partitionY, partitionIntervalX, partitionIntervalY;

    /**
     * Instantiates a new visualization partitioner.
     *
     * @param resolutionX the resolution X
     * @param resolutionY the resolution Y
     * @param partitionX the partition X
     * @param partitionY the partition Y
     * @throws Exception the exception
     */
    public VisualizationPartitioner(int resolutionX, int resolutionY, int partitionX, int partitionY)
            throws Exception
    {
        this.resolutionX = resolutionX;
        this.resolutionY = resolutionY;
        this.partitionX = partitionX;
        this.partitionY = partitionY;
        if (this.resolutionX % partitionX != 0 || this.resolutionY % partitionY != 0) {
            throw new Exception("[VisualizationPartitioner][Constructor] The given partition number fails to exactly divide the corresponding resolution axis.");
        }
        this.partitionIntervalX = this.resolutionX / this.partitionX;
        this.partitionIntervalY = this.resolutionY / this.partitionY;
    }

    /* (non-Javadoc)
     * @see org.apache.spark.Partitioner#getPartition(java.lang.Object)
     */
    @Override
    public int getPartition(Object key)
    {
        return ((Pixel) key).getCurrentPartitionId();
    }

    /* (non-Javadoc)
     * @see org.apache.spark.Partitioner#numPartitions()
     */
    @Override
    public int numPartitions()
    {
        return partitionX * partitionY;
    }

    /**
     * Assign partition I ds.
     *
     * @param pixelDoubleTuple2 the pixel double tuple 2
     * @param photoFilterRadius the photo filter radius
     * @return the list
     */
    public List<Tuple2<Pixel, Double>> assignPartitionIDs(Tuple2<Pixel, Double> pixelDoubleTuple2, int photoFilterRadius)
    {
        List<Tuple2<Pixel, Double>> duplicatePixelList = new ArrayList<Tuple2<Pixel, Double>>();
        //ArrayList<Integer> existingPartitionIds = new ArrayList<Integer>();
        // First, calculate the correct partition that the pixel belongs to
        int partitionId = CalculatePartitionId(this.resolutionX, this.resolutionY, this.partitionX, this.partitionY, pixelDoubleTuple2._1.getX(), pixelDoubleTuple2._1.getY());
        Pixel newPixel = new Pixel(pixelDoubleTuple2._1().getX(), pixelDoubleTuple2._1().getY(), resolutionX, resolutionY);
        newPixel.setCurrentPartitionId(partitionId);
        newPixel.setDuplicate(false);
        duplicatePixelList.add(new Tuple2<Pixel, Double>(newPixel, pixelDoubleTuple2._2()));
        //existingPartitionIds.add(partitionId);

        //Tuple2<Integer,Integer> pixelCoordinateInPartition = new Tuple2<Integer, Integer>(pixelDoubleTuple2._1().getX()%partitionIntervalX,pixelDoubleTuple2._1().getY()%partitionIntervalY);

        int[] boundaryCondition = {-1, 0, 1};
        for (int x : boundaryCondition) {
            for (int y : boundaryCondition) {
                int duplicatePartitionId = CalculatePartitionId(resolutionX, resolutionY, partitionX, partitionY,
                        pixelDoubleTuple2._1().getX() + x * photoFilterRadius, pixelDoubleTuple2._1().getY() + y * photoFilterRadius);
                if (duplicatePartitionId != partitionId && duplicatePartitionId >= 0) {
                    Pixel newPixelDuplicate = new Pixel(pixelDoubleTuple2._1().getX(), pixelDoubleTuple2._1().getY(), resolutionX, resolutionY);
                    newPixelDuplicate.setCurrentPartitionId(duplicatePartitionId);
                    newPixelDuplicate.setDuplicate(true);
                    duplicatePixelList.add(new Tuple2<Pixel, Double>(newPixelDuplicate, pixelDoubleTuple2._2()));
                }
            }
        }

		/*
		// Check whether this pixel may have impact on neighbors

		if(pixelCoordinateInPartition._1()<=0+photoFilterRadius || pixelCoordinateInPartition._1()>=partitionIntervalX-photoFilterRadius||pixelCoordinateInPartition._2()<=0+photoFilterRadius || pixelCoordinateInPartition._2()>=partitionIntervalY-photoFilterRadius)
		{
			// Second, calculate the partitions that the pixel duplicates should go to
			for (int x = -photoFilterRadius; x <= photoFilterRadius; x++) {
				for (int y = -photoFilterRadius; y <= photoFilterRadius; y++) {
					int neighborPixelX = pixelCoordinateInPartition._1()+x;
					int neighborPixelY = pixelCoordinateInPartition._2()+y;
					try {
						partitionId = RasterizationUtils.CalculatePartitionId(this.resolutionX,this.resolutionY,this.partitionX, this.partitionY, neighborPixelX, neighborPixelY);
						// This partition id is out of the image boundary
						if(partitionId<0) continue;
						if(!existingPartitionIds.contains(partitionId))
						{
							Pixel newPixelDuplicate = pixelDoubleTuple2._1();
							newPixelDuplicate.setCurrentPartitionId(partitionId);
							newPixelDuplicate.setDuplicate(true);
							existingPartitionIds.add(partitionId);
							duplicatePixelList.add(new Tuple2<Pixel, Double>(newPixelDuplicate, pixelDoubleTuple2._2()));
						}
					} catch (Exception e) {
						e.printStackTrace();
					}

				}
			}

		}*/

        return duplicatePixelList;
    }

    /**
     * Assign partition ID.
     *
     * @param pixelDoubleTuple2 the pixel double tuple 2
     * @return the tuple 2
     */
    public Tuple2<Pixel, Double> assignPartitionID(Tuple2<Pixel, Double> pixelDoubleTuple2)
    {
        int partitionId = CalculatePartitionId(this.resolutionX, this.resolutionY, this.partitionX, this.partitionY, pixelDoubleTuple2._1.getX(), pixelDoubleTuple2._1.getY());
        Pixel newPixel = pixelDoubleTuple2._1();
        newPixel.setCurrentPartitionId(partitionId);
        newPixel.setDuplicate(false);
        return new Tuple2<Pixel, Double>(newPixel, pixelDoubleTuple2._2());
    }

    /**
     * Calculate partition id.
     *
     * @param resolutionX the resolution X
     * @param resolutionY the resolution Y
     * @param partitionX the partition X
     * @param partitionY the partition Y
     * @param coordinateX the coordinate X
     * @param coordinateY the coordinate Y
     * @return the int
     */
    public static int CalculatePartitionId(int resolutionX, int resolutionY, int partitionX, int partitionY, int coordinateX, int coordinateY)
    {
        int partitionIntervalX = resolutionX / partitionX;
        int partitionIntervalY = resolutionY / partitionY;
        int partitionCoordinateX = coordinateX / partitionIntervalX;
        int partitionCoordinateY = coordinateY / partitionIntervalY;
        int partitionId = -1;
        try {
            partitionId = RasterizationUtils.Encode2DTo1DId(partitionX, partitionY, partitionCoordinateX, partitionY - 1 - partitionCoordinateY);
        }
        catch (Exception e) {
            // TODO Auto-generated catch block
            //e.printStackTrace();
        }
        return partitionId;
    }
}
