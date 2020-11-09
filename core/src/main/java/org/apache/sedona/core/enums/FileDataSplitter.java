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

package org.apache.sedona.core.enums;

import java.io.Serializable;

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
        for (FileDataSplitter me : FileDataSplitter.values()) {
            if (me.getDelimiter().equalsIgnoreCase(str) || me.name().equalsIgnoreCase(str)) { return me; }
        }
        throw new IllegalArgumentException("[" + FileDataSplitter.class + "] Unsupported FileDataSplitter:" + str);
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

