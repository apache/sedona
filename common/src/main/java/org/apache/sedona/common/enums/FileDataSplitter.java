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

package org.apache.sedona.common.enums;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

// TODO: Auto-generated Javadoc

/**
 * The Enum FileDataSplitter.
 */
public enum FileDataSplitter
        implements Serializable
{

    /**
     * The csv.
     */
    CSV(","),

    /**
     * The tsv.
     */
    TSV("\t"),

    /**
     * The geojson.
     */
    GEOJSON(""),

    /**
     * The wkt.
     */
    WKT("\t"),

    /**
     * The wkb.
     */
    WKB("\t"),

    COMMA(","),

    TAB("\t"),

    QUESTIONMARK("?"),

    SINGLEQUOTE("\'"),

    QUOTE("\""),

    UNDERSCORE("_"),

    DASH("-"),

    PERCENT("%"),

    TILDE("~"),

    PIPE("|"),

    SEMICOLON(";");

    /**
     * The splitter.
     */
    private final String splitter;

    // A lookup map for getting a FileDataSplitter from a delimiter, or its name
    private static final Map<String, FileDataSplitter> lookup = new HashMap<String, FileDataSplitter>();

    static {
        for (FileDataSplitter f : FileDataSplitter.values()) {
            lookup.put(f.getDelimiter(), f);
            lookup.put(f.name().toLowerCase(), f);
            lookup.put(f.name().toUpperCase(), f);
        }
    }
    /**
     * Instantiates a new file data splitter.
     *
     * @param splitter the splitter
     */
    FileDataSplitter(String splitter)
    {
        this.splitter = splitter;
    }

    /**
     * Gets the file data splitter.
     *
     * @param str the str
     * @return the file data splitter
     */
    public static FileDataSplitter getFileDataSplitter(String str)
    {
        FileDataSplitter f = lookup.get(str);
        if (f == null) {
            throw new IllegalArgumentException("[" + FileDataSplitter.class + "] Unsupported FileDataSplitter:" + str);
        }
        return f;
    }

    /**
     * Gets the delimiter.
     *
     * @return the delimiter
     */
    public String getDelimiter()
    {
        return this.splitter;
    }
}

