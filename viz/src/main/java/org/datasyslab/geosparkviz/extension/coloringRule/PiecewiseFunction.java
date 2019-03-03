/**
 * FILE: PiecewiseFunction.java
 * PATH: org.datasyslab.babylon.extension.coloringRule.PiecewiseFunction.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geosparkviz.extension.coloringRule;

import org.datasyslab.geosparkviz.core.ColoringRule;
import org.datasyslab.geosparkviz.core.GlobalParameter;

import java.awt.Color;

// TODO: Auto-generated Javadoc

/**
 * The Class PiecewiseFunction.
 */
public class PiecewiseFunction extends ColoringRule
{
    
    /* (non-Javadoc)
     * @see org.datasyslab.babylon.core.internalobject.ColoringRule#EncodeToRGB(java.lang.Double, org.datasyslab.babylon.core.parameters.GlobalParameter)
     */
    @Override
    public Integer EncodeToRGB(Double normailizedCount, GlobalParameter globalParameter) {
        int alpha = 150;
        Color[] colors = new Color[]{new Color(0,255,0,alpha),new Color(85,255,0,alpha),new Color(170,255,0,alpha),
                new Color(255,255,0,alpha),new Color(255,255,0,alpha),new Color(255,170,0,alpha),
                new Color(255,85,0,alpha),new Color(255,0,0,alpha)};
        if (normailizedCount == 0){
            return new Color(255,255,255,0).getRGB();
        }
        else if(normailizedCount<5)
        {
            return colors[0].getRGB();
        }
        else if(normailizedCount<15)
        {
            return colors[1].getRGB();
        }
        else if(normailizedCount<25)
        {
            return colors[2].getRGB();
        }
        else if(normailizedCount<35)
        {
            return colors[3].getRGB();
        }
        else if(normailizedCount<45)
        {
            return colors[4].getRGB();
        }
        else if(normailizedCount<60)
        {
            return colors[5].getRGB();
        }
        else if(normailizedCount<80)
        {
            return colors[6].getRGB();
        }
        else
        {
            return colors[7].getRGB();
        }
    }
}
