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
package org.apache.sedona.viz.sql.utils

object LineageDecoder {

  def apply(lineage: String): String = {
    var lineageIterator = lineage
    var partitionX = 0
    var partitionY = 0
    for (i <- 1 to lineage.length) {
      val offset = relativeOffset(lineageIterator.take(1).toInt)
      partitionX = partitionX * 2 + offset._1
      partitionY = partitionY * 2 + offset._2
      lineageIterator = lineageIterator.drop(1)
    }
    lineage.length + "-" + partitionX + "-" + partitionY
  }

  def relativeOffset(id: Int): (Int, Int) = {
    id match {
      case 0 => (0, 0)
      case 1 => (1, 0)
      case 2 => (0, 1)
      case 3 => (1, 1)
    }
  }

}
