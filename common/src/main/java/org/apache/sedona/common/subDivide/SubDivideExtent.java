/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sedona.common.subDivide;

import org.locationtech.jts.geom.Envelope;

public class SubDivideExtent {
    private double xMin;
    private double xMax;
    private double yMin;
    private double yMax;

    public SubDivideExtent(double xMin, double xMax, double yMin, double yMax) {
        this.xMin = xMin;
        this.xMax = xMax;
        this.yMin = yMin;
        this.yMax = yMax;
    }

    public SubDivideExtent(Envelope clip) {
        this.xMin = clip.getMinX();
        this.xMax = clip.getMaxX();
        this.yMin = clip.getMinY();
        this.yMax = clip.getMaxY();
    }

    public SubDivideExtent copy() {
        return new SubDivideExtent(this.xMin, this.xMax, this.yMin, this.yMax);
    }

    public double getxMin() {
        return xMin;
    }

    public SubDivideExtent setxMin(double xMin) {
        this.xMin = xMin;
        return this;
    }

    public double getxMax() {
        return xMax;
    }

    public SubDivideExtent setxMax(double xMax) {
        this.xMax = xMax;
        return this;
    }

    public double getyMin() {
        return yMin;
    }

    public SubDivideExtent setyMin(double yMin) {
        this.yMin = yMin;
        return this;
    }

    public double getyMax() {
        return yMax;
    }

    public SubDivideExtent setyMax(double yMax) {
        this.yMax = yMax;
        return this;
    }
}
