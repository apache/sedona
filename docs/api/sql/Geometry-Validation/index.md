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

# Geometry Validation

These functions test whether geometries are valid and can repair invalid geometries.

| Function | Description | Since |
| :--- | :--- | :--- |
| [ST_IsValid](ST_IsValid.md) | Test if a geometry is well-formed. The function can be invoked with just the geometry or with an additional flag (from `v1.5.1`). The flag alters the validity checking behavior. The flags parameter... | v1.0.0 |
| [ST_IsValidDetail](ST_IsValidDetail.md) | Returns a row, containing a boolean `valid` stating if a geometry is valid, a string `reason` stating why it is invalid and a geometry `location` pointing out where it is invalid. | v1.6.1 |
| [ST_IsValidReason](ST_IsValidReason.md) | Returns text stating if the geometry is valid. If not, it provides a reason why it is invalid. The function can be invoked with just the geometry or with an additional flag. The flag alters the val... | v1.5.1 |
| [ST_MakeValid](ST_MakeValid.md) | Given an invalid geometry, create a valid representation of the geometry. | v1.0.0 |
