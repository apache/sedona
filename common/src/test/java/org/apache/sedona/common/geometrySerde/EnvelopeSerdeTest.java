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
package org.apache.sedona.common.geometrySerde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Envelope;

public class EnvelopeSerdeTest {
  private final Kryo kryo = new Kryo();
  private final GeometrySerde geometrySerde = new GeometrySerde();

  private void testRoundTrip(Envelope env) {
    Output out = new Output(1024);
    geometrySerde.write(kryo, out, env);
    try (Input in = new Input(out.toBytes())) {
      Envelope env2 = (Envelope) geometrySerde.read(kryo, in, Envelope.class);
      Assert.assertEquals(env, env2);
    }
  }

  @Test
  public void testEnvelope() {
    testRoundTrip(new Envelope(10, 20, 30, 40));
  }

  @Test
  public void testSinglePointEnvelope() {
    testRoundTrip(new Envelope(10, 10, 10, 10));
  }

  @Test
  public void testNullEnvelope() {
    testRoundTrip(new Envelope());
  }
}
