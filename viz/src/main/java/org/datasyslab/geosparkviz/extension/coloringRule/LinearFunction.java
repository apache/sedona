/**
 * FILE: LinearFunction.java
 * PATH: org.datasyslab.babylon.extension.coloringRule.LinearFunction.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geosparkviz.extension.coloringRule;

import org.datasyslab.geosparkviz.core.ColoringRule;
import org.datasyslab.geosparkviz.core.GlobalParameter;

import java.awt.Color;

// TODO: Auto-generated Javadoc

/**
 * The Class LinearFunction.
 */
public class LinearFunction extends ColoringRule
{
    
    /* (non-Javadoc)
     * @see org.datasyslab.babylon.core.internalobject.ColoringRule#EncodeToRGB(java.lang.Double, org.datasyslab.babylon.core.parameters.GlobalParameter)
     */
    @Override
    public Integer EncodeToRGB(Double normailizedCount, GlobalParameter globalParameter) throws Exception {
        int red = 0;
        int green = 0;
        int blue = 0;
        if(globalParameter.controlColorChannel.equals(Color.RED))
        {
            red= globalParameter.useInverseRatioForControlColorChannel?255-normailizedCount.intValue():normailizedCount.intValue();
        }
        else if(globalParameter.controlColorChannel.equals(Color.GREEN))
        {
            green= globalParameter.useInverseRatioForControlColorChannel?255-normailizedCount.intValue():normailizedCount.intValue();
        }
        else if(globalParameter.controlColorChannel.equals(Color.BLUE))
        {
            blue= globalParameter.useInverseRatioForControlColorChannel?255-normailizedCount.intValue():normailizedCount.intValue();
        }
        else throw new Exception("[Babylon][GenerateColor] Unsupported changing color color type. It should be in R,G,B");

        
        if(normailizedCount==0)
        {
            return new Color(red,green,blue,0).getRGB();
        }
        
        return new Color(red,green,blue, globalParameter.colorAlpha).getRGB();
    }
}
