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
import org.apache.sedona.viz.extension.coloringRule.ColoringRuleFactory;
import org.apache.sedona.viz.extension.coloringRule.LinearFunction;
import org.apache.sedona.viz.extension.photoFilter.GaussianBlur;
import org.locationtech.jts.geom.Envelope;

import java.awt.Color;
import java.io.Serializable;

// TODO: Auto-generated Javadoc

/**
 * The Class GlobalParameter.
 */
public class GlobalParameter
        implements Serializable
{

    /**
     * The Constant logger.
     */
    final static Logger logger = Logger.getLogger(GlobalParameter.class);

    /**
     * The resolution X.
     */
    // Pixelize parameters: 5
    public int resolutionX = -1;

    /**
     * The resolution Y.
     */
    public int resolutionY = -1;

    /**
     * The reverse spatial coordinate.
     */
    public boolean reverseSpatialCoordinate = false;

    /**
     * The draw outline only.
     */
    public boolean drawOutlineOnly = false;

    /**
     * The min tree level.
     */
    public int minTreeLevel = 0;

    /**
     * The filter radius.
     */
    // Photo filter parameters: 2
    public int filterRadius = 0;

    /**
     * The photo filter.
     */
    public PhotoFilter photoFilter;

    /**
     * The sample amount.
     */
    // Coloring rule parameters: 6
    public double samplingFraction = 0.01;

    /**
     * The max pixel weight.
     */
    public int maxPixelWeight = -1;

    /**
     * The coloring rule.
     */
    public ColoringRule coloringRule;

    /**
     * The control color channel.
     */
    public Color controlColorChannel = Color.GREEN;

    /**
     * The use inverse ratio for control color channel.
     */
    public boolean useInverseRatioForControlColorChannel = false;

    /**
     * The color alpha.
     */
    public int colorAlpha = 255;

    /**
     * The dataset boundary.
     */
    // Dataset boundary: 1
    public Envelope datasetBoundary = new Envelope(-20026376.39, 20026376.39, -20048966.10, 20048966.10);

    /**
     * The partitions on single axis.
     */
    // Indirect parameters: 3
    public int partitionsOnSingleAxis = -1;

    /**
     * The partition interval X.
     */
    public double partitionIntervalX = -1.0;

    /**
     * The partition interval Y.
     */
    public double partitionIntervalY = -1.0;

    /**
     * The use user supplied resolution.
     */
    public boolean useUserSuppliedResolution = false;

    /**
     * The max partition tree level.
     */
    public int maxPartitionTreeLevel = 9;

    /**
     * The overwrite existing images.
     */
    public boolean overwriteExistingImages = true;

    private GlobalParameter(int resolutionX, int resolutionY, boolean reverseSpatialCoordinate, boolean drawOutlineOnly, int minTreeLevel,
            int filterRadius, PhotoFilter photoFilter, Double samplingFraction, int maxPixelWeight, ColoringRule coloringRule, Color controlColorChannel,
            boolean useInverseRatioForControlColorChannel, int colorAlpha, Envelope datasetBoundary, int maxPartitionTreeLevel, boolean overwriteExistingImages)
    {
        // Pixelize parameters: 5
        this.resolutionX = resolutionX;
        this.resolutionY = resolutionY;
        this.reverseSpatialCoordinate = reverseSpatialCoordinate;
        this.drawOutlineOnly = drawOutlineOnly;
        this.minTreeLevel = minTreeLevel;

        // Photo filter parameters: 2
        this.filterRadius = filterRadius;
        this.photoFilter = photoFilter;

        // Coloring rule parameters: 6
        this.samplingFraction = samplingFraction;
        this.maxPixelWeight = maxPixelWeight;
        this.coloringRule = coloringRule;
        this.controlColorChannel = controlColorChannel;
        this.useInverseRatioForControlColorChannel = useInverseRatioForControlColorChannel;
        this.colorAlpha = colorAlpha;

        // Dataset boundary: 1
        this.datasetBoundary = datasetBoundary;

        this.maxPartitionTreeLevel = maxPartitionTreeLevel;

        this.overwriteExistingImages = overwriteExistingImages;

        // Indirect parameters: 3
        this.partitionsOnSingleAxis = (int) Math.sqrt(Math.pow(4, minTreeLevel));
        if (this.resolutionX <= 0 && this.resolutionY <= 0) {
            // If not specify the resolution, then enforce the general image tile size which is 256*256.
            this.partitionIntervalX = 256;
            this.partitionIntervalY = 256;
            this.resolutionX = this.partitionsOnSingleAxis * 256;
            this.resolutionY = this.partitionsOnSingleAxis * 256;
            this.useUserSuppliedResolution = false;
        }
        else {
            this.partitionIntervalX = resolutionX * 1.0 / partitionsOnSingleAxis;
            this.partitionIntervalY = resolutionY * 1.0 / partitionsOnSingleAxis;
            this.useUserSuppliedResolution = true;
        }
    }

    /**
     * Gets the global parameter.
     *
     * @param parameterString the parameter string
     * @return the global parameter
     */
    public static GlobalParameter getGlobalParameter(String parameterString)
    {
        GlobalParameter globalParameter = getGlobalParameter();
        String[] parameters = parameterString.split(" ");
        for (String parameter : parameters) {
            globalParameter.set(parameter);
        }
        return globalParameter;
    }

    /**
     * Gets the global parameter.
     *
     * @return the global parameter
     */
    public static GlobalParameter getGlobalParameter()
    {
        GlobalParameter globalParameter = new GlobalParameter(0, 0, false, true, 0,
                0, new GaussianBlur(3), 0.01, -1, new LinearFunction(), Color.GREEN, false, 255, new Envelope(-20026376.39, 20026376.39, -20048966.10, 20048966.10), 9, true);
        return globalParameter;
    }

    /**
     * Gets the global parameter.
     *
     * @param resolutionX the resolution X
     * @param resolutionY the resolution Y
     * @param reverseSpatialCoordinate the reverse spatial coordinate
     * @param drawOutlineOnly the draw outline only
     * @param minTreeLevel the min tree level
     * @param filterRadius the filter radius
     * @param photoFilter the photo filter
     * @param samplingFraction the sampling fraction
     * @param maxPixelWeight the max pixel weight
     * @param coloringRule the coloring rule
     * @param controlColorChannel the control color channel
     * @param useInverseRatioForControlColorChannel the use inverse ratio for control color channel
     * @param colorAlpha the color alpha
     * @param datasetBoundary the dataset boundary
     * @param maxPartitionTreeLevel the max partition tree level
     * @param overwriteExistingImages the overwrite existing images
     * @return the global parameter
     */
    public static GlobalParameter getGlobalParameter(int resolutionX, int resolutionY, boolean reverseSpatialCoordinate, boolean drawOutlineOnly, int minTreeLevel,
            int filterRadius, PhotoFilter photoFilter, Double samplingFraction, int maxPixelWeight, ColoringRule coloringRule, Color controlColorChannel,
            boolean useInverseRatioForControlColorChannel, int colorAlpha, Envelope datasetBoundary, int maxPartitionTreeLevel, boolean overwriteExistingImages)
    {
        return new GlobalParameter(resolutionX, resolutionY, reverseSpatialCoordinate, drawOutlineOnly, minTreeLevel, filterRadius, photoFilter,
                samplingFraction, maxPixelWeight, coloringRule, controlColorChannel, useInverseRatioForControlColorChannel, colorAlpha, datasetBoundary, maxPartitionTreeLevel, overwriteExistingImages);
    }

    private boolean updateIndirectParameters()
    {
        this.partitionsOnSingleAxis = (int) Math.sqrt(Math.pow(4, this.minTreeLevel));
        if (this.useUserSuppliedResolution == false) {
            this.partitionIntervalX = 256;
            this.partitionIntervalY = 256;
            this.resolutionX = partitionsOnSingleAxis * 256;
            this.resolutionY = partitionsOnSingleAxis * 256;
        }
        else {
            this.partitionIntervalX = resolutionX * 1.0 / partitionsOnSingleAxis;
            this.partitionIntervalY = resolutionY * 1.0 / partitionsOnSingleAxis;
        }
        return true;
    }

    /**
     * Sets the dataset boundary.
     *
     * @param datasetBoundary the dataset boundary
     * @return true, if successful
     */
    public boolean setDatasetBoundary(Envelope datasetBoundary)
    {
        this.datasetBoundary = datasetBoundary;
        return true;
    }

    /**
     * Sets the coloring rule.
     *
     * @param coloringRule the coloring rule
     * @return true, if successful
     */
    public boolean setColoringRule(ColoringRule coloringRule)
    {
        this.coloringRule = coloringRule;
        return true;
    }

    /**
     * Sets the photo filter.
     *
     * @param photoFilter the photo filter
     * @return true, if successful
     */
    public boolean setPhotoFilter(PhotoFilter photoFilter)
    {
        this.photoFilter = photoFilter;
        return true;
    }

    /**
     * Sets the.
     *
     * @param keyValuePair the key value pair
     * @return true, if successful
     */
    public boolean set(String keyValuePair)
    {
        String[] keyValue = keyValuePair.split(":");
        this.set(keyValue[0], keyValue[1]);
        return true;
    }

    /**
     * Sets the.
     *
     * @param key the key
     * @param value the value
     * @return true, if successful
     */
    public boolean set(String key, String value)
    {
        // Pixelize parameters: 5
        if (key.equalsIgnoreCase("resolutionX") || key.equalsIgnoreCase("resX")) {
            this.resolutionX = Integer.parseInt(value);
            this.useUserSuppliedResolution = true;
            this.updateIndirectParameters();
        }
        else if (key.equalsIgnoreCase("resolutionY") || key.equalsIgnoreCase("resY")) {
            this.resolutionY = Integer.parseInt(value);
            this.useUserSuppliedResolution = true;
            this.updateIndirectParameters();
        }
        else if (key.equalsIgnoreCase("reverseSpatialCoordinate") || key.equalsIgnoreCase("revCoor")) {
            this.reverseSpatialCoordinate = Boolean.parseBoolean(value);
        }
        else if (key.equalsIgnoreCase("drawOutlineOnly") || key.equalsIgnoreCase("outline")) {
            this.drawOutlineOnly = Boolean.parseBoolean(value);
        }
        else if (key.equalsIgnoreCase("minTreeLevel") || key.equalsIgnoreCase("level")) {
            this.minTreeLevel = Integer.parseInt(value);
            this.updateIndirectParameters();
        }

        // Photo filter parameters: 2
        else if (key.equalsIgnoreCase("filterRadius") || key.equalsIgnoreCase("radius")) {
            this.filterRadius = Integer.parseInt(value);
        }

        // Coloring rule parameters: 6
        else if (key.equalsIgnoreCase("samplingfraction") || key.equalsIgnoreCase("fraction")) {
            this.samplingFraction = Double.parseDouble(value);
        }
        else if (key.equalsIgnoreCase("maxPixelWeight")) {
            this.maxPixelWeight = Integer.parseInt(value);
        }
        else if (key.equalsIgnoreCase("coloringRule")) {
            this.coloringRule = ColoringRuleFactory.getColoringRule(value);
        }
        else if (key.equalsIgnoreCase("controlColorChannel") || key.equalsIgnoreCase("ccolor")) {
            this.controlColorChannel = Color.getColor(value);
        }
        else if (key.equalsIgnoreCase("useInverseRatioForControlColorChannel") || key.equalsIgnoreCase("invColor")) {
            this.useInverseRatioForControlColorChannel = Boolean.parseBoolean(value);
        }
        else if (key.equalsIgnoreCase("colorAlpha") || key.equalsIgnoreCase("alpha")) {
            this.colorAlpha = Integer.parseInt(value);
        }

        // Dataset boundary: 1
        else if (key.equalsIgnoreCase("datasetBoundary") || key.equalsIgnoreCase("bound")) {
            String[] coordinates = value.split(",");
            double minX = Double.parseDouble(coordinates[0]);
            double maxX = Double.parseDouble(coordinates[1]);
            double minY = Double.parseDouble(coordinates[2]);
            double maxY = Double.parseDouble(coordinates[3]);
            this.datasetBoundary = new Envelope(minX, maxX, minY, maxY);
        }
        else if (key.equalsIgnoreCase("maxPartitionTreeLevel") || key.equalsIgnoreCase("maxtreelevel")) {
            this.maxPartitionTreeLevel = Integer.parseInt(value);
        }
        else if (key.equalsIgnoreCase("overwriteexistingimages") || key.equalsIgnoreCase("overwrite")) {
            this.overwriteExistingImages = Boolean.parseBoolean(value);
        }
        else {
            logger.error(new Exception("[Babylon][set] No such parameter: " + key));
        }
        return true;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        String parameterString = "\n";

        // Pixelize parameters: 5
        parameterString += "resolutionX: " + this.resolutionX + "\n";
        parameterString += "resolutionY: " + this.resolutionY + "\n";
        parameterString += "reverseSpatialCoordinate: " + this.reverseSpatialCoordinate + "\n";
        parameterString += "drawOutlineOnly: " + this.drawOutlineOnly + "\n";
        parameterString += "minTreeLevel: " + this.minTreeLevel + "\n";

        // Photo filter parameters: 2
        parameterString += "filterRadius: " + this.filterRadius + "\n";
        parameterString += "photoFilter: " + this.photoFilter.getClass().getName() + "\n";

        // Coloring rule parameters: 6
        parameterString += "samplingFraction: " + this.samplingFraction + "\n";
        parameterString += "maxPixelWeight: " + this.maxPixelWeight + "\n";
        parameterString += "coloringRule: " + this.coloringRule.getClass().getName() + "\n";
        parameterString += "controlColorChannel: " + this.controlColorChannel + "\n";
        parameterString += "useInverseRatioForControlColorChannel: " + this.useInverseRatioForControlColorChannel + "\n";
        parameterString += "colorAlpha: " + this.colorAlpha + "\n";

        // Dataset boundary: 1
        parameterString += "datasetBoundary: " + this.datasetBoundary.toString() + "\n";
        parameterString += "maxPartitionTreeLevel: " + this.maxPartitionTreeLevel + "\n";
        parameterString += "overwriteExistingImages: " + this.overwriteExistingImages + "\n";

        // Indirect parameters: 3
        parameterString += "partitionIntervalX: " + this.partitionIntervalX + "\n";
        parameterString += "partitionIntervalY: " + this.partitionIntervalY + "\n";
        parameterString += "partitionsOnSingleAxis: " + this.partitionsOnSingleAxis + "\n";
        return parameterString;
    }
}
