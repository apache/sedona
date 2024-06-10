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
package org.apache.sedona.common.utils;

import org.apache.sedona.common.Functions;
import org.locationtech.jts.geom.Geometry;

public class ValidDetail {
    public final boolean valid;
    public final String reason;
    public final Geometry location;

    public ValidDetail(boolean valid, String reason, Geometry location) {
        this.valid = valid;
        this.reason = reason;
        this.location = location;
    }

    @Override
    public String toString() {
        return String.format("Valid: %b\nReason: %s\nLocation: %s", valid, reason, Functions.asWKT(location));
    }

    public boolean equals(ValidDetail other) {
        return this.valid == other.valid &&
                (this.reason == null &&
                this.location == null) ||
                (this.reason.equals(other.reason) &&
                this.location.equals(other.location));
    }
}
