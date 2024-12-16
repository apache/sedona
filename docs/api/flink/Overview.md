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

# Introduction

SedonaSQL supports SQL/MM Part3 Spatial SQL Standard. Please read the programming guide: [Sedona with Flink SQL app](../../tutorial/flink/sql.md).

Sedona includes SQL operators as follows.

- Constructor: Construct a Geometry given an input string or coordinates
  - Example: ST_GeomFromWKT (string). Create a Geometry from a WKT String.
- Function: Execute a function on the given column or columns
  - Example: ST_Distance (A, B). Given two Geometry A and B, return the Euclidean distance of A and B.
- Aggregator: Return a single aggregated value on the given column
  - Example: ST_Envelope_Aggr (Geometry column). Given a Geometry column, calculate the entire envelope boundary of this column.
- Predicate: Execute a logic judgement on the given columns and return true or false
  - Example: ST_Contains (A, B). Check if A fully contains B. Return "True" if yes, else return "False".
