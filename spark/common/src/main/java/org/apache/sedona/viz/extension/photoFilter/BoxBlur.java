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
package org.apache.sedona.viz.extension.photoFilter;

import org.apache.sedona.viz.core.PhotoFilter;

// TODO: Auto-generated Javadoc

/**
 * The Class BoxBlur.
 */
public class BoxBlur
        extends PhotoFilter
{

    /**
     * Instantiates a new box blur.
     *
     * @param filterRadius the filter radius
     */
    public BoxBlur(int filterRadius)
    {
        super(filterRadius);
        for (int x = -filterRadius; x <= filterRadius; x++) {
            for (int y = -filterRadius; y <= filterRadius; y++) {
                convolutionMatrix[x + filterRadius][y + filterRadius] = 1.0 / this.convolutionMatrix.length;
            }
        }
    }
}
