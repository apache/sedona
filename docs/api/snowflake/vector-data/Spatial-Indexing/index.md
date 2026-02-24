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

# Spatial Indexing

These functions work with spatial indexing systems including Bing Tiles, H3, S2, and GeoHash.

| Function | Description | Since |
| :--- | :--- | :--- |
| [ST_BingTile](ST_BingTile.md) | Creates a Bing Tile quadkey from tile XY coordinates and a zoom level. |  |
| [ST_BingTileAt](ST_BingTileAt.md) | Returns the Bing Tile quadkey for a given point (longitude, latitude) at a specified zoom level. |  |
| [ST_BingTileCellIDs](ST_BingTileCellIDs.md) | Returns an array of Bing Tile quadkey strings that cover the given geometry at the specified zoom level. |  |
| [ST_BingTilePolygon](ST_BingTilePolygon.md) | Returns the bounding polygon (Geometry) of the Bing Tile identified by the given quadkey. |  |
| [ST_BingTilesAround](ST_BingTilesAround.md) | Returns an array of Bing Tile quadkey strings representing the neighborhood tiles around the tile that contains the given point (longitude, latitude) at the specified zoom level. Returns the 3×3 ne... |  |
| [ST_BingTileToGeom](ST_BingTileToGeom.md) | Returns a GeometryCollection of Polygons for the corresponding Bing Tile quadkeys. |  |
| [ST_BingTileX](ST_BingTileX.md) | Returns the tile X coordinate of the Bing Tile identified by the given quadkey. |  |
| [ST_BingTileY](ST_BingTileY.md) | Returns the tile Y coordinate of the Bing Tile identified by the given quadkey. |  |
| [ST_BingTileZoomLevel](ST_BingTileZoomLevel.md) | Returns the zoom level of the Bing Tile identified by the given quadkey. |  |
| [ST_GeoHashNeighbor](ST_GeoHashNeighbor.md) | Returns the neighbor geohash cell in the given direction. Valid directions are: `n`, `ne`, `e`, `se`, `s`, `sw`, `w`, `nw` (case-insensitive). |  |
| [ST_GeoHashNeighbors](ST_GeoHashNeighbors.md) | Returns the 8 neighboring geohash cells of a given geohash string. The result is an array of 8 geohash strings in the order: N, NE, E, SE, S, SW, W, NW. |  |
| [ST_S2CellIDs](ST_S2CellIDs.md) | Cover the geometry with Google S2 Cells, return the corresponding cell IDs with the given level. The level indicates the [size of cells](https://s2geometry.io/resources/s2cell_statistics.html). Wit... |  |
