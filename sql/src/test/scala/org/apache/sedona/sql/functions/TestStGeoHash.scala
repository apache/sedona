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
package org.apache.sedona.sql.functions

import org.apache.sedona.sql.{GeometrySample, TestBaseScala}
import org.scalatest.GivenWhenThen

class TestStGeoHash extends TestBaseScala with GeometrySample with GivenWhenThen{

  describe("should correctly calculate st geohash function"){
    it("should return geohash"){
      Given("geometry dataframe")

      When("")

      Then("")

    }

    it("should return null if column value is null"){
      Given("geometry df with null elements")

      When("calculating geohash")

      Then("result should be null")

    }

    it("should return geohash truncated to max value"){
      Given("geometry df")

      When("calculating geohash with precision exceeding maximum allowed")

      Then("geohash should be truncated to maximum possible precision")

    }

    it("should return null when precision is negative or equal 0"){

    }
  }
}
