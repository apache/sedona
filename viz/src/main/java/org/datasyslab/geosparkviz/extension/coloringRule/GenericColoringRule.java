/**
 * FILE: GenericColoringRule
 * PATH: org.datasyslab.babylon.extension.coloringRule.GenericColoringRule
 * Copyright (c) GeoSpark Development Team
 * <p>
 * MIT License
 * <p>
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * <p>
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * <p>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package org.datasyslab.geosparkviz.extension.coloringRule;

import java.awt.Color;

public class GenericColoringRule {
    public static Integer EncodeToRGB(Double normailizedCount) {
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
