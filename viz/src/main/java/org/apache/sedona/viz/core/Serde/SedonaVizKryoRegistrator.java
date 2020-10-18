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
package org.apache.sedona.viz.core.Serde;

import com.esotericsoftware.kryo.Kryo;
import org.apache.log4j.Logger;
import org.apache.sedona.core.serde.SedonaKryoRegistrator;
import org.apache.sedona.viz.core.ImageSerializableWrapper;
import org.apache.sedona.viz.utils.Pixel;
import org.apache.spark.serializer.KryoRegistrator;

public class SedonaVizKryoRegistrator
        implements KryoRegistrator
{
    final static Logger log = Logger.getLogger(SedonaVizKryoRegistrator.class);

    @Override
    public void registerClasses(Kryo kryo)
    {
        SedonaKryoRegistrator sedonaKryoRegistrator = new SedonaKryoRegistrator();
        ImageWrapperSerializer imageWrapperSerializer = new ImageWrapperSerializer();
        PixelSerializer pixelSerializer = new PixelSerializer();
        sedonaKryoRegistrator.registerClasses(kryo);
        log.info("Registering custom serializers for visualization related types");
        kryo.register(ImageSerializableWrapper.class, imageWrapperSerializer);
        kryo.register(Pixel.class, pixelSerializer);
    }
}
