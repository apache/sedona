This page explains the basic concepts of Affine Transformation in Sedona Raster.

## Affine Transformations

Affine transformations are a fundamental concept in computer graphics, geometry, and image processing that involve manipulating an object in a way that preserves lines and parallelism (but not necessarily distances and angles). These transformations are linear transformations followed by a translation, which means they can translate, scale, rotate, and shear objects without altering the relative arrangement of points, lines, or planes.

### Components of Affine Transformations

Affine transformations can be represented as a matrix operation. In two-dimensional space, a typical affine transformation matrix is a 3x3 matrix as follows:

```
| ScaleX  SkewX   TranslationX |
| SkewY   ScaleY  TranslationY |
| 0       0       1            |
```

Here, `ScaleX, ScaleY, SkewX, SkewY, TranslationX,` and `TranslationY` are parameters that define the transformation:

- `ScaleX` and `ScaleY` are scaling factors for the x and y axes, respectively.
- `SkewX` and `SkewY` introduce shearing and are responsible for "skewing" the shape.
- `TranslationX` and `TranslationY` are translation parameters that move the shape in the x and y directions, respectively.

### Types of Affine Transformations

1. **Translation**: Moves every point of a figure or space by the same distance in a given direction. This primarily affects the `TranslationX` and `TranslationY` components.

2. **Scaling**: Multiplies the coordinates of each point by a constant (ScaleX for x-axis and ScaleY for y-axis), enlarging or reducing its size. Scaling can be uniform (the same factor for both axes) or non-uniform (different factors for each axis).

3. **Rotation**: Rotates the object about a point (usually the origin or a specified point). This can be expressed through combinations of `ScaleX, ScaleY, SkewX,` and `SkewY` where these parameters are derived from the cosine and sine of the rotation angle.

4. **Shearing**: Transforms parallel lines to still be parallel but moves them so that they are no longer perpendicular to their original orientations. This affects the `SkewX` and `SkewY` components.

5. **Reflection**: Flips the object over a specified axis, which can be achieved by combining scaling and rotation.

### Mathematical Properties

- **Collinearity and Concurrency**: Affine transformations preserve points on a line (collinearity) and the intersection of lines (concurrency).
- **Ratios of Segments**: They also preserve the ratios of distances between points lying on a straight line.

## Components of Affine Transformations

In affine transformations, which are integral to manipulating graphics, images, and geometric data, the terms **ScaleX**, **ScaleY**, **SkewX**, and **SkewY** refer to specific types of transformations that alter the shape and position of objects:

### ScaleX and ScaleY

- **ScaleX**: This parameter represents the scaling factor along the x-axis. It modifies the width of an image or object. Values greater than 1 increase the width, values less than 1 decrease it, and negative values reflect the object along the x-axis while scaling.

- **ScaleY**: This parameter represents the scaling factor along the y-axis. It affects the height of the object. Similarly to ScaleX, values greater than 1 enlarge the object vertically, values less than 1 reduce it, and negative values invert it along the y-axis.

### SkewX and SkewY

- **SkewX**: This parameter is used to skew or shear the object along the x-axis. It shifts each point's x-coordinate in proportion to its y-coordinate, creating a slanting effect. This transformation is useful for creating the illusion of depth or perspective in 2D representations.

- **SkewY**: Corresponding to SkewX, SkewY skews the object along the y-axis. It alters each point's y-coordinate relative to its x-coordinate, which also creates a slanting effect, but in the vertical direction.

These transformations are typically used together in a transformation matrix, which allows them to be applied to objects in a combined and coherent way. Here's a typical representation of such a matrix:

```
| ScaleX  SkewX   TranslationX |
| SkewY   ScaleY  TranslationY |
| 0       0       1            |
```

These parameters can be combined in various ways to perform complex transformations such as rotations, translations, scaling, and shearing of images or shapes in both 2D and 3D graphics applications.
