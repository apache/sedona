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

import org.apache.sedona.viz.utils.Pixel;
import org.apache.sedona.viz.utils.RasterizationUtils;
import org.apache.spark.Partitioner;
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

        Tuple2<Integer, Integer> partitionId2d = Calculate2DPartitionId(resolutionX, resolutionY, partitionX, partitionY, coordinateX, coordinateY);
        int partitionId = -1;
        try {
            partitionId = RasterizationUtils.Encode2DTo1DId(partitionX, partitionY, partitionId2d._1, partitionId2d._2);
        }
        catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return partitionId;
    }

    /**
     * Calculate the 2D partition ID, in a <x, y> format
     *
     * @param resolutionX
     * @param resolutionY
     * @param partitionX
     * @param partitionY
     * @param coordinateX
     * @param coordinateY
     * @return
     */
    public static Tuple2<Integer, Integer> Calculate2DPartitionId(int resolutionX, int resolutionY, int partitionX, int partitionY, int coordinateX, int coordinateY)
    {
        int partitionIntervalX = resolutionX / partitionX;
        int partitionIntervalY = resolutionY / partitionY;
        int partitionCoordinateX = coordinateX / partitionIntervalX;
        int partitionCoordinateY = partitionY - 1 - coordinateY / partitionIntervalY;
        return new Tuple2<>(partitionCoordinateX, partitionCoordinateY);
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
        int partitionId = CalculatePartitionId(this.resolutionX, this.resolutionY, this.partitionX, this.partitionY, (int) pixelDoubleTuple2._1.getX(), (int) pixelDoubleTuple2._1.getY());
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
                        (int) pixelDoubleTuple2._1().getX() + x * photoFilterRadius, (int) pixelDoubleTuple2._1().getY() + y * photoFilterRadius);
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
        int partitionId = CalculatePartitionId(this.resolutionX, this.resolutionY, this.partitionX, this.partitionY, (int) pixelDoubleTuple2._1.getX(), (int) pixelDoubleTuple2._1.getY());
        Pixel newPixel = pixelDoubleTuple2._1();
        newPixel.setCurrentPartitionId(partitionId);
        newPixel.setDuplicate(false);
        return new Tuple2<Pixel, Double>(newPixel, pixelDoubleTuple2._2());
    }
}
