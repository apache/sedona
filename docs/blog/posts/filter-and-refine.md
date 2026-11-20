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

*Sedona internals, one idea at a time.*

A spatial join asks one question again and again: is this address inside this city? Overture Maps lists 305 cities and towns and 3,036,650 addresses in Washington State, so the join has 926 million pairs to ask about. The exact answer is slow, because the boundary of Tacoma alone is 3,152 points. Apache Sedona asks a cheaper question first, and this short post shows how.

![Two maps of Tacoma, Washington. On the left, the filter step: every address inside the bounding box of the city is dark, 159,915 addresses. On the right, the refine step: the city boundary is drawn, 106,230 addresses inside it are blue and 53,685 outside it are orange](filter-and-refine-cover.png)

<!-- more -->

## Filter: ask the box

Every shape has a bounding box, the smallest rectangle that holds it. A box is four numbers, and testing a point against it takes four comparisons. An address outside the box of Tacoma is outside Tacoma. An address inside the box may be in Tacoma, so it becomes a candidate. The box of Tacoma holds 159,915 addresses.

## The tree: skip most boxes

Testing every address against 305 boxes is still a lot of work. Sedona stores the boxes in an R-tree. The tree groups up to ten nearby boxes into a bigger box, then groups those boxes again, until one box covers the state. The 305 city boxes become 35 groups, then 4, then 1.

A search starts at the top and opens only the boxes that hold the address.

![Four maps that zoom in step by step. The first shows 4 boxes over Washington State, one of them blue. The second shows the 10 boxes inside it, one blue. The third shows 10 city boxes, three blue. The fourth shows the outlines of Tacoma, University Place and Fircrest, with Fircrest filled blue around a star](filter-and-refine-tree.png)

The star is one address in Fircrest, a small city that sits inside the box of Tacoma. The search tests 4 boxes, then 10, then 10. After 24 box tests, three candidates are left: Tacoma, University Place and Fircrest.

## Refine: ask the shape

Only the candidates get the exact test against the real boundary. The address is inside the box of Tacoma and outside Tacoma. It is inside Fircrest. Three exact tests, one match.

## The whole join

Sedona 1.9.1 counts its own work during a join. For all of Washington, it reported 2,627,358 candidates and 1,865,052 matches.

![A large grey square stands for 926,178,250 possible pairs. A small orange square in its corner stands for the 2,627,358 pairs that pass the box test, and a blue square inside it for the 1,865,052 pairs that pass the exact test](filter-and-refine-pairs.png)

The filter step left 0.28% of all pairs. The refine step ran the slow test on those and kept 71% of them.

Filter is cheap and touches many pairs. Refine is exact and touches few. Every spatial join in Sedona has this shape. The bounding boxes in a GeoParquet footer, from [One Sort, Many Indexes](https://sedona.apache.org/latest/blog/2026/11/06/one-sort-many-indexes/), are the same idea one level up: there the box lets a reader skip a whole row group.

*Cities and addresses from Overture Maps (divisions and addresses themes, release 2026-10-21.0). The tree is the R-tree in Sedona 1.9.1, built over the 305 city boxes. Counts from one `ST_Contains` join on Sedona 1.9.1 on a laptop.*
