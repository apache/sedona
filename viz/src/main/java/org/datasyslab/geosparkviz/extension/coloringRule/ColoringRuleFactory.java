/**
 * FILE: ColoringRuleFactory.java
 * PATH: org.datasyslab.babylon.extension.coloringRule.ColoringRuleFactory.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geosparkviz.extension.coloringRule;

import org.apache.log4j.Logger;
import org.datasyslab.geosparkviz.core.ColoringRule;

// TODO: Auto-generated Javadoc

/**
 * A factory for creating ColoringRule objects.
 */
public class ColoringRuleFactory {
    
    /** The Constant logger. */
    final static Logger logger = Logger.getLogger(ColoringRuleFactory.class);
    
    /**
     * Gets the coloring rule.
     *
     * @param ruleName the rule name
     * @return the coloring rule
     */
    public static ColoringRule getColoringRule(String ruleName)
    {
        if (ruleName.equalsIgnoreCase("linear"))
        {
            return new LinearFunction();
        }
        else if (ruleName.equalsIgnoreCase("piecewise"))
        {
            return new PiecewiseFunction();
        }
        else
        {
            logger.error("[Babylon][getColoringRule] No such coloring rule: "+ruleName);
            return null;
        }
    }
}
