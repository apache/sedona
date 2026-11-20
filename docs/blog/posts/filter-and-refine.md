---
date:
  created: 2026-11-20
links:
  - Sedona query optimizer: https://sedona.apache.org/latest/api/sql/Optimizer/
  - "One Sort, Many Indexes": https://sedona.apache.org/latest/blog/2026/11/06/one-sort-many-indexes/
authors:
  - jia
title: "Ask the Box First: Filter and Refine in Apache Sedona"
slug: filter-and-refine
---

# Ask the Box First: Filter and Refine in Apache Sedona

A spatial join asks one question again and again: does this road touch this city? Overture Maps lists 305 cities and towns and 1,495,982 road segments in Washington State, so the join has 456 million pairs to ask about. The exact answer is slow, because the boundary of Tacoma alone is 3,152 points and a road is a line with many points of its own. Apache Sedona asks a cheaper question first, and this short post shows how.

![Two maps of the roads of Tacoma, Washington. On the left, the filter step: the bounding box of the city is yellow and the 46,832 roads whose boxes overlap it are black. On the right, the refine step: the city boundary is drawn, the 30,937 roads that touch the city are blue and the 15,895 that do not are pink](filter-and-refine-cover.png)

<!-- more -->

## Filter: ask the box

Every shape has a bounding box, the smallest rectangle that holds it. A box is four numbers, and two boxes are compared with four comparisons. When two boxes are apart, the shapes inside them are apart. When two boxes overlap, the shapes may touch, so the pair becomes a candidate. The box of Tacoma overlaps the boxes of 46,832 roads.

## The tree: skip most boxes

Comparing every road with 305 city boxes is still a lot of work. Sedona stores the boxes in an R-tree. The tree groups up to ten nearby boxes into a bigger box, then groups those boxes again, until one box covers the state. The 305 city boxes become 35 groups, then 4, then 1.

A search starts at the top and opens only the boxes that overlap the box of the road.

![On the left, a map of Washington State with the four top boxes of the tree labeled A to D, the smaller boxes inside them, and one small box in yellow. On the right, a close view of one road and four cities: University Place and Steilacoom in blue, Tacoma and Lakewood in pink. Below, the same boxes drawn as a tree: a root, four boxes, 35 boxes and 305 city boxes, with the path through box B highlighted](filter-and-refine-tree.png)

The star is Chambers Creek Road West, 2.9 km long. The search tests 4 boxes, then 10, then 10. After 24 box tests, four candidates are left: Tacoma, University Place, Steilacoom and Lakewood.

## Refine: ask the shape

Only the candidates get the exact test against the real boundary. The road runs through University Place and Steilacoom. Its box overlaps the boxes of Tacoma and Lakewood, and the road never reaches either city. Four exact tests, two matches.

## The whole join

Sedona 1.9.1 counts its own work during a join. For all of Washington, it reported 1,171,972 candidates and 840,156 matches.

![A large grey square stands for 456,274,510 possible pairs. A small pink square in its corner stands for the 1,171,972 pairs whose boxes overlap, and a blue square inside it for the 840,156 pairs that touch](filter-and-refine-pairs.png)

The filter step left 0.26% of all pairs. The refine step ran the slow test on those and kept 72% of them.

Filter is cheap and sees many pairs. Refine is exact and sees few. Every spatial join in Sedona has this shape. The bounding boxes in a GeoParquet footer, from [One Sort, Many Indexes](https://sedona.apache.org/latest/blog/2026/11/06/one-sort-many-indexes/), are the same idea one level up: there the box lets a reader skip a whole row group.

*Cities and roads from Overture Maps (divisions and transportation themes, release 2026-10-21.0). The tree is the R-tree in Sedona 1.9.1, built over the 305 city boxes. Counts from one `ST_Intersects` join on Sedona 1.9.1 on a laptop.*
