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

# Raster Output

These functions convert raster data to various output formats for visualization.

| Function | Description | Since |
| :--- | :--- | :--- |
| [RS_AsBase64](RS_AsBase64.md) | Returns a base64 encoded string of the given raster. If the datatype is integral then this function internally takes the first 4 bands as RGBA, and converts them to the PNG format, finally produces... | v1.5.0 |
| [RS_AsImage](RS_AsImage.md) | Returns a HTML that when rendered using an HTML viewer or via a Jupyter Notebook, displays the raster as a square image of side length `imageWidth`. Optionally, an imageWidth parameter can be passe... | v1.5.0 |
| [RS_AsMatrix](RS_AsMatrix.md) | Returns a string, that when printed, outputs the raster band as a pretty printed 2D matrix. All the values of the raster are cast to double for the string. RS_AsMatrix allows specifying the number ... |  |
