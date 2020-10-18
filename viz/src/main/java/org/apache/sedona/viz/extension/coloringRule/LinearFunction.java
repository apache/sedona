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

import org.apache.sedona.viz.core.ColoringRule;
import org.apache.sedona.viz.core.GlobalParameter;

import java.awt.Color;

// TODO: Auto-generated Javadoc

/**
 * The Class LinearFunction.
 */
public class LinearFunction
        extends ColoringRule
{

    /* (non-Javadoc)
     * @see org.datasyslab.babylon.core.internalobject.ColoringRule#EncodeToRGB(java.lang.Double, org.datasyslab.babylon.core.parameters.GlobalParameter)
     */
    @Override
    public Integer EncodeToRGB(Double normailizedCount, GlobalParameter globalParameter)
            throws Exception
    {
        int red = 0;
        int green = 0;
        int blue = 0;
        if (globalParameter.controlColorChannel.equals(Color.RED)) {
            red = globalParameter.useInverseRatioForControlColorChannel ? 255 - normailizedCount.intValue() : normailizedCount.intValue();
        }
        else if (globalParameter.controlColorChannel.equals(Color.GREEN)) {
            green = globalParameter.useInverseRatioForControlColorChannel ? 255 - normailizedCount.intValue() : normailizedCount.intValue();
        }
        else if (globalParameter.controlColorChannel.equals(Color.BLUE)) {
            blue = globalParameter.useInverseRatioForControlColorChannel ? 255 - normailizedCount.intValue() : normailizedCount.intValue();
        }
        else { throw new Exception("[Babylon][GenerateColor] Unsupported changing color color type. It should be in R,G,B"); }

        if (normailizedCount == 0) {
            return new Color(red, green, blue, 0).getRGB();
        }

        return new Color(red, green, blue, globalParameter.colorAlpha).getRGB();
    }
}
