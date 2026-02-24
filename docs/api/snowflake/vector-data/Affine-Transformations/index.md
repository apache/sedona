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

# Affine Transformations

These functions change the position and shape of geometries using affine transformations.

| Function | Description | Since |
| :--- | :--- | :--- |
| [ST_Affine](ST_Affine.md) | Apply an affine transformation to the given geometry. |  |
| [ST_Rotate](ST_Rotate.md) | Rotates a geometry by a specified angle in radians counter-clockwise around a given origin point. The origin for rotation can be specified as either a POINT geometry or x and y coordinates. If the ... |  |
| [ST_RotateX](ST_RotateX.md) | Performs a counter-clockwise rotation of the specified geometry around the X-axis by the given angle measured in radians. |  |
| [ST_RotateY](ST_RotateY.md) | Performs a counter-clockwise rotation of the specified geometry around the Y-axis by the given angle measured in radians. |  |
| [ST_Scale](ST_Scale.md) | This function scales the geometry to a new size by multiplying the ordinates with the corresponding scaling factors provided as parameters `scaleX` and `scaleY`. |  |
| [ST_ScaleGeom](ST_ScaleGeom.md) | This function scales the input geometry (`geometry`) to a new size. It does this by multiplying the coordinates of the input geometry with corresponding values from another geometry (`factor`) repr... |  |
| [ST_Translate](ST_Translate.md) | Returns the input geometry with its X, Y and Z coordinates (if present in the geometry) translated by deltaX, deltaY and deltaZ (if specified) |  |
