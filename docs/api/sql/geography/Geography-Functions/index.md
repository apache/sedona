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

# Geography Functions

These functions operate on geography type objects.

| Function | Description | Since |
| :--- | :--- | :--- |
| [ST_AsEWKT](ST_AsEWKT.md) | Return the Extended Well-Known Text representation of a geography. EWKT is an extended version of WKT which includes the SRID of the geography. The format originated in PostGIS but is supported by ... | v1.8.0 |
| [ST_Envelope](ST_Envelope.md) | This function returns the bounding box (envelope) of A. It's important to note that the bounding box is calculated using a cylindrical topology, not a spherical one. If the envelope crosses the ant... | v1.8.0 |
