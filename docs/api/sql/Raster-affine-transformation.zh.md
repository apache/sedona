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

本页介绍 Sedona Raster 中仿射变换（Affine Transformation）的基本概念。

![Raster_Affine_Transformation](../../image/Raster_Affine_Transformation/Raster_Affine_Transformation.svg "Raster Affine Transformation")

## 仿射变换

仿射变换是计算机图形学、几何与图像处理中的一个基础概念，指以保持直线性与平行性（但不一定保持距离与角度）的方式对对象进行变换。这类变换是一种线性变换叠加上一次平移，因此能够在不改变点、线、面相对排列的前提下，对对象进行平移、缩放、旋转与剪切。

### 仿射变换的组成

仿射变换可以表示为矩阵运算。在二维空间中，典型的仿射变换矩阵是一个 3x3 矩阵，如下所示：

```
| ScaleX  SkewX   TranslationX |
| SkewY   ScaleY  TranslationY |
| 0       0       1            |
```

这里，`ScaleX、ScaleY、SkewX、SkewY、TranslationX` 和 `TranslationY` 是定义该变换的参数：

- `ScaleX` 和 `ScaleY` 分别是 x 轴和 y 轴方向的缩放因子。
- `SkewX` 和 `SkewY` 引入剪切效果，使形状产生“倾斜”。
- `TranslationX` 和 `TranslationY` 是平移参数，分别将形状沿 x 与 y 方向移动。

### 仿射变换的类型

1. **平移（Translation）**：将图形或空间中的每个点沿指定方向移动相同的距离。主要影响 `TranslationX` 与 `TranslationY` 分量。

2. **缩放（Scaling）**：将每个点的坐标乘以一个常数（x 轴方向使用 ScaleX，y 轴方向使用 ScaleY），从而放大或缩小其尺寸。缩放可以是均匀的（两个轴使用相同因子），也可以是非均匀的（两轴因子不同）。

3. **旋转（Rotation）**：围绕某一点（通常是原点或指定点）对对象进行旋转。可以通过 `ScaleX、ScaleY、SkewX` 与 `SkewY` 的组合来表示，这些参数由旋转角的余弦和正弦推导得到。

4. **剪切（Shearing）**：将平行线变换为仍然平行的直线，但移动它们的位置，使其不再垂直于原来的方向。影响 `SkewX` 与 `SkewY` 分量。

5. **反射（Reflection）**：沿指定轴翻转对象，可通过缩放与旋转的组合来实现。

### 数学性质

- **共线性与共点性**：仿射变换保持点的共线性（点位于同一直线上）以及直线的共点性（直线的交点）。
- **线段比例**：仿射变换还保持同一直线上各点之间距离的比例。

## 仿射变换的组成

在仿射变换中（这是处理图形、图像与几何数据的基础），**ScaleX**、**ScaleY**、**SkewX** 和 **SkewY** 这几个术语分别指代会改变对象形状与位置的特定变换：

### ScaleX 与 ScaleY

- **ScaleX**：表示沿 x 轴的缩放因子。它改变图像或对象的宽度。大于 1 的取值会增大宽度，小于 1 的取值会减小宽度，负值则会在缩放的同时将对象沿 x 轴反射。

- **ScaleY**：表示沿 y 轴的缩放因子。它影响对象的高度。与 ScaleX 类似，大于 1 的取值会沿垂直方向放大对象，小于 1 的取值会缩小对象，负值则会沿 y 轴翻转对象。

### SkewX 与 SkewY

- **SkewX**：用于沿 x 轴方向对对象进行剪切或扭曲。它按每个点的 y 坐标的比例平移其 x 坐标，从而产生倾斜效果。该变换可用于在 2D 表达中营造深度或透视的视觉效果。

- **SkewY**：与 SkewX 相对应，SkewY 沿 y 轴方向对对象进行剪切。它根据每个点的 x 坐标改变其 y 坐标，同样产生倾斜效果，但方向是垂直的。

这些变换通常组合在一个变换矩阵中使用，使它们能够以协同且一致的方式作用于对象。下面是这种矩阵的典型表达：

```
| ScaleX  SkewX   TranslationX |
| SkewY   ScaleY  TranslationY |
| 0       0       1            |
```

这些参数可以以不同方式组合，从而在 2D 与 3D 图形应用中对图像或形状执行旋转、平移、缩放与剪切等复杂变换。
