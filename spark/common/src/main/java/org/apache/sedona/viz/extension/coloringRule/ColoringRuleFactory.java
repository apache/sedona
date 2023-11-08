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

import org.apache.log4j.Logger;
import org.apache.sedona.viz.core.ColoringRule;

// TODO: Auto-generated Javadoc

/**
 * A factory for creating ColoringRule objects.
 */
public class ColoringRuleFactory
{

    /**
     * The Constant logger.
     */
    final static Logger logger = Logger.getLogger(ColoringRuleFactory.class);

    /**
     * Gets the coloring rule.
     *
     * @param ruleName the rule name
     * @return the coloring rule
     */
    public static ColoringRule getColoringRule(String ruleName)
    {
        if (ruleName.equalsIgnoreCase("linear")) {
            return new LinearFunction();
        }
        else if (ruleName.equalsIgnoreCase("piecewise")) {
            return new PiecewiseFunction();
        }
        else {
            logger.error("[Babylon][getColoringRule] No such coloring rule: " + ruleName);
            return null;
        }
    }
}
