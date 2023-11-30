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
package org.apache.sedona.viz.extension.coloringRule;

import java.awt.Color;

public class GenericColoringRule
{
    public static Integer EncodeToRGB(Double normailizedCount)
    {
        int alpha = 150;
        Color[] colors = new Color[] {new Color(0, 255, 0, alpha), new Color(85, 255, 0, alpha), new Color(170, 255, 0, alpha),
                new Color(255, 255, 0, alpha), new Color(255, 255, 0, alpha), new Color(255, 170, 0, alpha),
                new Color(255, 85, 0, alpha), new Color(255, 0, 0, alpha)};
        if (normailizedCount == 0) {
            return new Color(255, 255, 255, 0).getRGB();
        }
        else if (normailizedCount < 5) {
            return colors[0].getRGB();
        }
        else if (normailizedCount < 15) {
            return colors[1].getRGB();
        }
        else if (normailizedCount < 25) {
            return colors[2].getRGB();
        }
        else if (normailizedCount < 35) {
            return colors[3].getRGB();
        }
        else if (normailizedCount < 45) {
            return colors[4].getRGB();
        }
        else if (normailizedCount < 60) {
            return colors[5].getRGB();
        }
        else if (normailizedCount < 80) {
            return colors[6].getRGB();
        }
        else {
            return colors[7].getRGB();
        }
    }
}
