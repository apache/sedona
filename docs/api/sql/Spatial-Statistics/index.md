<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# Spatial Statistics

These functions compute spatial statistics and spatial weights.

| Function | Description | Since |
| :--- | :--- | :--- |
| [ST_BinaryDistanceBandColumn](ST_BinaryDistanceBandColumn.md) | Introduction: Returns a `weights` column containing every record in a dataframe within a specified `threshold` distance. | v1.7.1 |
| [ST_GLocal](ST_GLocal.md) | Runs Getis and Ord's G Local (Gi or Gi*) statistic on the geometry given the `weights` and `level`. | v1.7.1 |
| [ST_WeightedDistanceBandColumn](ST_WeightedDistanceBandColumn.md) | Introduction: Returns a `weights` column containing every record in a dataframe within a specified `threshold` distance. | v1.7.1 |
