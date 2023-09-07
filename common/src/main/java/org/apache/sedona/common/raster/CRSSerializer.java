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
package org.apache.sedona.common.raster;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.geotools.referencing.CRS;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.zip.DeflaterInputStream;
import java.util.zip.DeflaterOutputStream;

/**
 * There won't be too many distinct CRSes in a typical application, so we can cache the serialized form
 * of CRS objects to speed up the serialization/deserialization process. The serialized CRS is also compressed
 * to reduce the memory footprint. Typical serialized CRS size is around 50KB, and the compressed size is around 10KB.
 * According to our assumption, each distinct CRS will only be serialized once, so the computation cost to compress
 * the serialized CRS is negligible.
 */
public class CRSSerializer {
    private CRSSerializer() {}

    private static class CRSKey {
        private final CoordinateReferenceSystem crs;
        private final int hashCode;

        CRSKey(CoordinateReferenceSystem crs) {
            this.crs = crs;
            this.hashCode = crs.hashCode();
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof CRSKey) {
                return CRS.equalsIgnoreMetadata(crs, ((CRSKey) obj).crs);
            } else {
                return false;
            }
        }
    }

    private static final LoadingCache<CRSKey, byte[]> crsSerializationCache = Caffeine.newBuilder()
            .maximumSize(100)
            .build(CRSSerializer::doSerializeCRS);

    private static final LoadingCache<ByteBuffer, CoordinateReferenceSystem> crsDeserializationCache = Caffeine.newBuilder()
            .maximumSize(100)
            .build(CRSSerializer::doDeserializeCRS);

    public static byte[] serialize(CoordinateReferenceSystem crs) {
        return crsSerializationCache.get(new CRSKey(crs));
    }

    public static CoordinateReferenceSystem deserialize(byte[] bytes) {
        return crsDeserializationCache.get(ByteBuffer.wrap(bytes));
    }

    private static byte[] doSerializeCRS(CRSKey crsKey) throws IOException {
        CoordinateReferenceSystem crs = crsKey.crs;
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DeflaterOutputStream dos = new DeflaterOutputStream(bos);
            ObjectOutputStream oos = new ObjectOutputStream(dos)) {
            oos.writeObject(crs);
            oos.flush();
            dos.finish();
            byte[] res = bos.toByteArray();
            crsDeserializationCache.put(ByteBuffer.wrap(res), crs);
            return res;
        }
    }

    private static CoordinateReferenceSystem doDeserializeCRS(ByteBuffer byteBuffer) throws IOException, ClassNotFoundException {
        byte[] bytes = byteBuffer.array();
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
             DeflaterInputStream dis = new DeflaterInputStream(bis);
             ObjectInputStream ois = new ObjectInputStream(dis)) {
            CoordinateReferenceSystem crs = (CoordinateReferenceSystem) ois.readObject();
            crsSerializationCache.put(new CRSKey(crs), bytes);
            return crs;
        }
    }
}
