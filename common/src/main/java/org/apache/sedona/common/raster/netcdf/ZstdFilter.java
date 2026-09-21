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
package org.apache.sedona.common.raster.netcdf;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import ucar.nc2.filter.Filter;
import ucar.nc2.filter.FilterProvider;

/**
 * Zstandard filter (HDF5 registered filter id 32015) for netCDF-Java. netCDF-Java does not ship
 * one, so it is registered through {@code META-INF/services/ucar.nc2.filter.FilterProvider}. Each
 * chunk is a single Zstandard frame, as written by the HDF5 and netCDF-C zstd plugins.
 */
public class ZstdFilter extends Filter {
  static final String NAME = "zstd";
  static final int ID = 32015;

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getId() {
    return ID;
  }

  @Override
  public byte[] encode(byte[] dataIn) {
    return Zstd.compress(dataIn);
  }

  @Override
  public byte[] decode(byte[] dataIn) throws IOException {
    // Streaming decode handles frames that do not record their decompressed size
    try (ZstdInputStream in = new ZstdInputStream(new ByteArrayInputStream(dataIn));
        ByteArrayOutputStream out = new ByteArrayOutputStream(dataIn.length)) {
      byte[] buffer = new byte[(int) ZstdInputStream.recommendedDOutSize()];
      int n;
      while ((n = in.read(buffer)) != -1) {
        out.write(buffer, 0, n);
      }
      return out.toByteArray();
    }
  }

  public static class Provider implements FilterProvider {
    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public int getId() {
      return ID;
    }

    @Override
    public Filter create(Map<String, Object> properties) {
      return new ZstdFilter();
    }
  }
}
