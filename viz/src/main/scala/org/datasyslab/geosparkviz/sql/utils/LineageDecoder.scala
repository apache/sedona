/**
  * FILE: LineageDecoder
  * Copyright (c) 2015 - 2019 GeoSpark Development Team
  *
  * MIT License
  *
  * Permission is hereby granted, free of charge, to any person obtaining a copy
  * of this software and associated documentation files (the "Software"), to deal
  * in the Software without restriction, including without limitation the rights
  * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  * copies of the Software, and to permit persons to whom the Software is
  * furnished to do so, subject to the following conditions:
  *
  * The above copyright notice and this permission notice shall be included in all
  * copies or substantial portions of the Software.
  *
  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  * SOFTWARE.
  */
package org.datasyslab.geosparkviz.sql.utils

object LineageDecoder {

  def apply(lineage:String): String = {
    var lineageIterator = lineage
    var partitionX = 0
    var partitionY = 0
    for (i <- 1 to lineage.length){
      val offset = relativeOffset(lineageIterator.take(1).toInt)
      partitionX = partitionX*2 + offset._1
      partitionY = partitionY*2 + offset._2
      lineageIterator = lineageIterator.drop(1)
    }
    lineage.length+"-"+partitionX+"-"+partitionY
  }

  def relativeOffset(id:Int): (Int, Int) = {
    id match {
      case 0 => (0,0)
      case 1 => (1,0)
      case 2 => (0,1)
      case 3 => (1,1)
    }
  }

}
