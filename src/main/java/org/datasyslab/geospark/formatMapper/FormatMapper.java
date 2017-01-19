/**
 * FILE: FormatMapper.java
 * PATH: org.datasyslab.geospark.formatMapper.FormatMapper.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.formatMapper;

import java.io.Serializable;

import org.datasyslab.geospark.enums.FileDataSplitter;

// TODO: Auto-generated Javadoc
/**
 * The Class FormatMapper.
 */
public abstract class FormatMapper implements Serializable{

	
    /** The start offset. */
    public Integer startOffset = 0;

    /** The end offset. */
    public Integer endOffset = -1; /* If the initial value is negative, GeoSpark will consider each field as a spatial attribute if the target object is LineString or Polygon. */
    
    /** The splitter. */
    public FileDataSplitter splitter = FileDataSplitter.CSV;

    /** The carry input data. */
    public boolean carryInputData = false;
    
    /**
     * Instantiates a new format mapper.
     *
     * @param startOffset the start offset
     * @param endOffset the end offset
     * @param Splitter the splitter
     * @param carryInputData the carry input data
     */
    public FormatMapper(Integer startOffset, Integer endOffset, FileDataSplitter Splitter, boolean carryInputData) {
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.splitter = Splitter;
        this.carryInputData = carryInputData;
    }

    /**
     * Instantiates a new format mapper.
     *
     * @param Splitter the splitter
     * @param carryInputData the carry input data
     */
    public FormatMapper(FileDataSplitter Splitter, boolean carryInputData) {
        this.splitter = Splitter;
        this.carryInputData = carryInputData;
    }
}
