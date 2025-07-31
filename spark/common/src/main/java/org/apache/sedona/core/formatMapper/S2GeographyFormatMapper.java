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
package org.apache.sedona.core.formatMapper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.sedona.common.S2Geography.S2Geography;
import org.apache.sedona.common.S2Geography.S2Geography.GeographyKind;
import org.apache.sedona.common.enums.FileDataSplitter;
import org.apache.sedona.common.utils.S2GeographyFormatUtils;
import org.apache.spark.api.java.function.FlatMapFunction;

public class S2GeographyFormatMapper<T extends S2Geography> extends S2GeographyFormatUtils
    implements FlatMapFunction<Iterator<String>, T> {

  /**
   * Instantiates a new format mapper.
   *
   * @param startOffset the start offset
   * @param endOffset the end offset
   * @param splitter the splitter
   * @param carryInputData the carry input data
   * @param geographyKind
   */
  public S2GeographyFormatMapper(
      int startOffset,
      int endOffset,
      FileDataSplitter splitter,
      boolean carryInputData,
      GeographyKind geographyKind) {
    super(startOffset, endOffset, splitter, carryInputData, geographyKind);
  }

  /**
   * Instantiates a new format mapper. This is extensively used in SedonaSQL.
   *
   * @param splitter
   * @param carryInputData
   */
  public S2GeographyFormatMapper(FileDataSplitter splitter, boolean carryInputData) {
    super(splitter, carryInputData);
  }

  /**
   * This format mapper is used in SedonaSQL.
   *
   * @param splitter
   * @param carryInputData
   * @param geographyKind
   */
  public S2GeographyFormatMapper(
      FileDataSplitter splitter, boolean carryInputData, GeographyKind geographyKind) {
    super(splitter, carryInputData, geographyKind);
  }

  @Override
  public Iterator<T> call(Iterator<String> stringIterator) throws Exception {
    List<T> result = new ArrayList<>();
    while (stringIterator.hasNext()) {
      String line = stringIterator.next();
      addGeometry(readGeometry(line), result);
    }
    return result.iterator();
  }
}
