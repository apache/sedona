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
package org.apache.sedona.sql.datasources.spider

import org.locationtech.jts.geom.util.AffineTransformation
import org.locationtech.jts.geom.Envelope

/**
 * Represents an affine transformation matrix.
 *
 * The transformation matrix is represented as:
 * | scaleX  skewX   translateX |
 * |:---------------------------|
 * | skewY   scaleY  translateY |
 * | 0       0       1          |
 */
case class AffineTransform(
    translateX: Double,
    translateY: Double,
    scaleX: Double,
    scaleY: Double,
    skewX: Double,
    skewY: Double) {
  def toJTS: Option[AffineTransformation] = {
    if (isIdentity) {
      None
    } else {
      Some(
        new AffineTransformation(
          scaleX, // m00
          skewX, // m01
          translateX, // m02
          skewY, // m10
          scaleY, // m11
          translateY // m12
        ))
    }
  }

  private def isIdentity: Boolean = {
    translateX == 0 && translateY == 0 && scaleX == 1 && scaleY == 1 && skewX == 0 && skewY == 0
  }

  def transform(box: Envelope): AffineTransform = {
    // Cascade an affine transform A that transforms the unit box [0, 1] x [0, 1] into the desired box
    // after self, the final transform is self * A

    // Calculate the scale factors
    val scaleX = box.getWidth
    val scaleY = box.getHeight

    // Calculate the translation factors
    val translateX = box.getMinX
    val translateY = box.getMinY

    // Multiply the transformation matrices in the correct order (self * A)
    val newScaleX = this.scaleX * scaleX
    val newScaleY = this.scaleY * scaleY
    val newSkewX = this.skewX * scaleY
    val newSkewY = this.skewY * scaleX
    val newTranslateX = this.scaleX * translateX + this.skewX * translateY + this.translateX
    val newTranslateY = this.skewY * translateX + this.scaleY * translateY + this.translateY

    // Now we get the new transformation
    AffineTransform(newTranslateX, newTranslateY, newScaleX, newScaleY, newSkewX, newSkewY)
  }
}
