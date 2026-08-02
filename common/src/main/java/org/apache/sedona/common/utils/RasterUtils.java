/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sedona.common.utils;

import com.sun.media.imageioimpl.common.BogusColorSpace;
import java.awt.Color;
import java.awt.Point;
import java.awt.Transparency;
import java.awt.color.ColorSpace;
import java.awt.geom.Point2D;
import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.ComponentColorModel;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.util.*;
import javax.media.jai.RasterFactory;
import javax.media.jai.RenderedImageAdapter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.stat.descriptive.rank.Median;
import org.apache.sedona.common.Functions;
import org.apache.sedona.common.FunctionsGeoTools;
import org.apache.sedona.common.raster.RasterAccessors;
import org.apache.sedona.common.raster.RasterEditors;
import org.apache.sedona.common.raster.RasterPredicates;
import org.geotools.api.coverage.SampleDimensionType;
import org.geotools.api.coverage.grid.GridEnvelope;
import org.geotools.api.metadata.spatial.PixelOrientation;
import org.geotools.api.referencing.FactoryException;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.api.referencing.operation.MathTransform;
import org.geotools.api.referencing.operation.TransformException;
import org.geotools.api.util.InternationalString;
import org.geotools.coverage.Category;
import org.geotools.coverage.CoverageFactoryFinder;
import org.geotools.coverage.GridSampleDimension;
import org.geotools.coverage.TypeMap;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.geometry.Position2D;
import org.geotools.referencing.crs.DefaultEngineeringCRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.geotools.util.NumberRange;
import org.locationtech.jts.geom.Geometry;

/** Utility functions for working with GridCoverage2D objects. */
public class RasterUtils {
  private RasterUtils() {}

  private static final GridCoverageFactory gridCoverageFactory =
      CoverageFactoryFinder.getGridCoverageFactory(null);

  /**
   * Create a new empty raster from the given WritableRaster object.
   *
   * @param raster The raster object to be wrapped as an image.
   * @param gridGeometry The grid geometry of the raster.
   * @param bands The bands of the raster.
   * @return A new GridCoverage2D object.
   */
  public static GridCoverage2D create(
      WritableRaster raster, GridGeometry2D gridGeometry, GridSampleDimension[] bands) {
    return create(raster, gridGeometry, bands, null);
  }

  /**
   * @param raster WriteableRaster to be used while creating the new raster.
   * @param gridGeometry2D gridGeometry2D to be used while cloning
   * @param bands bands to be used while cloning
   * @param referenceRaster referenceRaster to clone from
   * @param noDataValue noDataValue (if any) to be applied to the bands. If provided null. bands are
   *     unchanged.
   * @param keepMetadata if passed true, clone all possible metadata from the referenceRaster.
   *     keepMetaData controls the presence/absence of the following metadata of the
   *     referenceRasterObject: raster name: Name of the raster (GridCoverage2D level) raster
   *     properties: A Map of raster and image properties combined. raster sources
   * @return A cloned raster
   */
  public static GridCoverage2D clone(
      WritableRaster raster,
      GridGeometry2D gridGeometry2D,
      GridSampleDimension[] bands,
      GridCoverage2D referenceRaster,
      Double noDataValue,
      boolean keepMetadata) {
    Map propertyMap = null;
    if (keepMetadata) {
      propertyMap = referenceRaster.getProperties();
    }
    ColorModel originalColorModel = referenceRaster.getRenderedImage().getColorModel();
    if (Objects.isNull(gridGeometry2D)) {
      gridGeometry2D = referenceRaster.getGridGeometry();
    }
    int numBand = raster.getNumBands();
    int rasterDataType = raster.getDataBuffer().getDataType();
    ColorModel colorModel;
    if (originalColorModel.isCompatibleRaster(raster)) {
      colorModel = originalColorModel;
    } else {
      final ColorSpace cs = new BogusColorSpace(numBand);
      final int[] nBits = new int[numBand];
      Arrays.fill(nBits, DataBuffer.getDataTypeSize(rasterDataType));
      colorModel =
          new ComponentColorModel(cs, nBits, false, true, Transparency.OPAQUE, rasterDataType);
    }
    if (noDataValue != null) {
      GridSampleDimension[] newBands = new GridSampleDimension[numBand];
      for (int k = 0; k < numBand; k++) {
        if (bands != null) {
          newBands[k] = createSampleDimensionWithNoDataValue(bands[k], noDataValue);
        } else {
          newBands[k] = createSampleDimensionWithNoDataValue("band_" + k, noDataValue);
        }
      }
      bands = newBands;
    }

    GridCoverage2D[] referenceRasterSources =
        keepMetadata ? referenceRaster.getSources().toArray(new GridCoverage2D[0]) : null;
    CharSequence rasterName = keepMetadata ? referenceRaster.getName() : "genericCoverage";

    final RenderedImage image = new BufferedImage(colorModel, raster, false, null);
    return gridCoverageFactory.create(
        rasterName, image, gridGeometry2D, bands, referenceRasterSources, propertyMap);
  }

  public static GridCoverage2D clone(
      WritableRaster raster,
      GridSampleDimension[] bands,
      GridCoverage2D referenceRaster,
      Double noDataValue,
      boolean keepMetadata) {
    return RasterUtils.clone(raster, null, bands, referenceRaster, noDataValue, keepMetadata);
  }

  public static GridCoverage2D clone(
      RenderedImage image,
      GridSampleDimension[] bands,
      GridCoverage2D referenceRaster,
      Double noDataValue,
      boolean keepMetadata) {
    return RasterUtils.clone(image, null, bands, referenceRaster, noDataValue, keepMetadata);
  }

  /**
   * @param image Rendered image to create the raster from
   * @param gridGeometry2D gridGeometry2D to be used while cloning
   * @param bands bands to be used while cloning
   * @param referenceRaster referenceRaster to clone from
   * @param noDataValue noDataValue (if any) to be applied to the bands. If provided null. bands are
   *     unchanged.
   * @param keepMetadata if passed true, clone all possible metadata from the referenceRaster.
   *     keepMetaData controls the presence/absence of the following metadata of the
   *     referenceRasterObject: raster name: Name of the raster (GridCoverage2D level) raster
   *     properties: A Map of raster and image properties combined. raster sources
   * @return A cloned raster
   */
  public static GridCoverage2D clone(
      RenderedImage image,
      GridGeometry2D gridGeometry2D,
      GridSampleDimension[] bands,
      GridCoverage2D referenceRaster,
      Double noDataValue,
      boolean keepMetadata) {
    int numBand = image.getSampleModel().getNumBands();
    if (Objects.isNull(gridGeometry2D)) {
      gridGeometry2D = referenceRaster.getGridGeometry();
    }
    if (noDataValue != null) {
      GridSampleDimension[] newBands = new GridSampleDimension[numBand];
      for (int k = 0; k < numBand; k++) {
        if (bands != null) {
          newBands[k] = createSampleDimensionWithNoDataValue(bands[k], noDataValue);
        } else {
          newBands[k] = createSampleDimensionWithNoDataValue("band_" + k, noDataValue);
        }
      }
      bands = newBands;
    }
    GridCoverage2D[] referenceRasterSources =
        keepMetadata ? referenceRaster.getSources().toArray(new GridCoverage2D[0]) : null;
    Map propertyMap = null;
    if (keepMetadata) {
      propertyMap = referenceRaster.getProperties();
    }
    CharSequence rasterName = keepMetadata ? referenceRaster.getName() : "genericCoverage";
    return gridCoverageFactory.create(
        rasterName, image, gridGeometry2D, bands, referenceRasterSources, propertyMap);
  }

  /**
   * Create a new empty raster from the given WritableRaster object.
   *
   * @param raster The raster object to be wrapped as an image.
   * @param gridGeometry The grid geometry of the raster.
   * @param bands The bands of the raster.
   * @param noDataValue the noDataValue (if any) to be applied to all bands. If provided null, bands
   *     are unchanged. keepMetaData controls the presence/absence of the following metadata of the
   *     referenceRasterObject: raster name: Name of the raster (GridCoverage2D level) raster
   *     properties: A Map of raster and image properties combined. raster sources
   * @return A new GridCoverage2D object.
   */
  public static GridCoverage2D create(
      WritableRaster raster,
      GridGeometry2D gridGeometry,
      GridSampleDimension[] bands,
      Double noDataValue) {
    return create(raster, gridGeometry, bands, noDataValue, null);
  }

  public static GridCoverage2D create(
      WritableRaster raster,
      GridGeometry2D gridGeometry,
      GridSampleDimension[] bands,
      Double noDataValue,
      Map properties) {
    int numBand = raster.getNumBands();
    int rasterDataType = raster.getDataBuffer().getDataType();

    // Construct a color model for the rendered image. This color model should be able to be
    // serialized and
    // deserialized. The color model object automatically constructed by grid coverage factory may
    // not be
    // serializable, please refer to https://issues.apache.org/jira/browse/SEDONA-319 for more
    // details.
    final ColorSpace cs = new BogusColorSpace(numBand);
    final int[] nBits = new int[numBand];
    Arrays.fill(nBits, DataBuffer.getDataTypeSize(rasterDataType));
    ColorModel colorModel =
        new ComponentColorModel(cs, nBits, false, true, Transparency.OPAQUE, rasterDataType);

    if (noDataValue != null) {
      GridSampleDimension[] newBands = new GridSampleDimension[numBand];
      for (int k = 0; k < numBand; k++) {
        if (bands != null) {
          newBands[k] = createSampleDimensionWithNoDataValue(bands[k], noDataValue);
        } else {
          newBands[k] = createSampleDimensionWithNoDataValue("band_" + k, noDataValue);
        }
      }
      bands = newBands;
    }

    final RenderedImage image = new BufferedImage(colorModel, raster, false, null);
    return gridCoverageFactory.create(
        "genericCoverage", image, gridGeometry, bands, null, properties);
  }

  public static GridCoverage2D create(
      RenderedImage image,
      GridGeometry2D gridGeometry,
      GridSampleDimension[] bands,
      Double noDataValue) {
    int numBand = image.getSampleModel().getNumBands();
    if (noDataValue != null) {
      GridSampleDimension[] newBands = new GridSampleDimension[numBand];
      for (int k = 0; k < numBand; k++) {
        if (bands != null) {
          newBands[k] = createSampleDimensionWithNoDataValue(bands[k], noDataValue);
        } else {
          newBands[k] = createSampleDimensionWithNoDataValue("band_" + k, noDataValue);
        }
      }
      bands = newBands;
    }
    return gridCoverageFactory.create("genericCoverage", image, gridGeometry, bands, null, null);
  }

  /**
   * Create a sample dimension using a given sampleDimension as template, with the give no data
   * value.
   *
   * @param sampleDimension The sample dimension to be used as template.
   * @param noDataValue The no data value.
   * @return A new sample dimension with the given no data value.
   */
  public static GridSampleDimension createSampleDimensionWithNoDataValue(
      GridSampleDimension sampleDimension, double noDataValue) {
    return createSampleDimensionWithNoDataValue(
        sampleDimension, noDataValue, sampleDimension.getSampleDimensionType());
  }

  /**
   * Create a sample dimension using a given sampleDimension as template, with the given no data
   * value encoded for an explicit sample dimension type.
   *
   * <p>The type matters because the no data value is wrapped into a {@link Number} of that type. It
   * is only worth passing explicitly when the template's own type does not describe the pixels the
   * sample dimension will end up attached to — deserializing a raster whose band data was replaced
   * by a Python UDF, for instance, where the template comes from the source raster but the pixels
   * may have a wider type.
   *
   * @param sampleDimension The sample dimension to be used as template.
   * @param noDataValue The no data value.
   * @param sampleDimensionType The type to encode the no data value for.
   * @return A new sample dimension with the given no data value.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static GridSampleDimension createSampleDimensionWithNoDataValue(
      GridSampleDimension sampleDimension,
      double noDataValue,
      SampleDimensionType sampleDimensionType) {
    // Encode the value for the sample dimension type first: a float band stores 99.2 as
    // 99.19999694824219, and the split ranges and new category must agree on the encoded value,
    // not the requested one.
    Number wrappedNoData = TypeMap.wrapSample(noDataValue, sampleDimensionType, false);
    double effectiveNoDataValue = wrappedNoData.doubleValue();

    // Preserve the existing categories when their declared scalar already agrees. In particular,
    // callers that copy an existing range-valued NODATA category pass its minimum here and expect
    // the range to remain intact. Wire-level authoritative overrides remove that category before
    // calling this shared helper.
    if (Double.compare(getNoDataValue(sampleDimension), effectiveNoDataValue) == 0) {
      return sampleDimension;
    }

    NumberRange noDataRange =
        new NumberRange(wrappedNoData.getClass(), wrappedNoData, wrappedNoData);

    String description = sampleDimension.getDescription().toString();
    List<Category> categories = sampleDimension.getCategories();
    double offset = sampleDimension.getOffset();
    double scale = sampleDimension.getScale();

    // Copy existing categories. If the category contains noDataValue, split it into two categories.
    List<Category> newCategories = new ArrayList<>(categories.size());
    for (Category category : categories) {
      if (category.getName().equals(Category.NODATA.getName())) {
        // The existing no data category is being replaced wholesale, so drop it. This has to
        // happen before the split below: a range-valued no data category that contains the new
        // value would otherwise be split into no data-named fragments, and getNoDataValue() reads
        // the first such category, reporting the fragment's minimum instead of the value asked for.
        continue;
      }
      NumberRange<? extends Number> range = category.getRange();
      if (range.contains(wrappedNoData)) {
        // NumberRange performs the split in the widest operand type, so a float output retains
        // Float fragments while a byte category widened to double can represent a fractional
        // NODATA value. It can return representationally empty fragments at exclusive boundaries;
        // compare their effective included endpoints before constructing a Category.
        for (NumberRange<?> fragment : range.subtract(noDataRange)) {
          if (fragment.getMinimum(true) <= fragment.getMaximum(true)) {
            newCategories.add(
                new Category(
                    category.getName(), category.getColors(), fragment, category.isQuantitative()));
          }
        }
      } else {
        // This category does not contain no data value, just keep it as is.
        newCategories.add(category);
      }
    }

    // Add the no data value as a new category
    newCategories.add(
        new Category(
            Category.NODATA.getName(),
            new Color(0, 0, 0, 0),
            new NumberRange(wrappedNoData.getClass(), wrappedNoData, wrappedNoData)));

    return new GridSampleDimension(
        description, newCategories.toArray(new Category[0]), scale, offset);
  }

  public static GridSampleDimension createSampleDimensionWithNoDataValue(
      String description, double noDataValue) {
    Category noDataCategory =
        new Category(
            Category.NODATA.getName(),
            new Color(0, 0, 0, 0),
            new NumberRange<>(java.lang.Double.class, noDataValue, noDataValue));
    Category[] categories = new Category[] {noDataCategory};
    return new GridSampleDimension(description, categories, null);
  }

  /**
   * Remove no data value from the given sample dimension.
   *
   * @param sampleDimension The sample dimension to be processed.
   * @return A new sample dimension without no data value, or the original sample dimension if it
   *     does not contain no data value.
   */
  public static GridSampleDimension removeNoDataValue(GridSampleDimension sampleDimension) {
    String description = sampleDimension.getDescription().toString();
    List<Category> categories = sampleDimension.getCategories();
    List<Category> newCategories = new ArrayList<>(categories.size());
    InternationalString noDataCategoryName = Category.NODATA.getName();
    for (Category category : categories) {
      if (!category.getName().equals(noDataCategoryName)) {
        newCategories.add(category);
      }
    }

    if (newCategories.size() == categories.size()) {
      // Nothing changed, return the original sample dimension
      return sampleDimension;
    }

    double offset = sampleDimension.getOffset();
    double scale = sampleDimension.getScale();
    return new GridSampleDimension(
        description, newCategories.toArray(new Category[0]), scale, offset);
  }

  /**
   * Get the no data value from the given sample dimension. Please use this method to retrieve the
   * no data value of a raster band, instead of {@link GridSampleDimension#getNoDataValues()}. The
   * reason is that the latter method has a strange semantics: it treats whatever qualitative
   * categories as no data value, which is not what we want. Additionally, the GeoTiff writer and
   * ArcGrid writer uses the same algorithm as our method for finding no data values when writing
   * the metadata of raster bands.
   *
   * @param sampleDimension The sample dimension to be processed.
   * @return The no data value, or {@link Double#NaN} if the sample dimension does not contain no
   *     data value.
   */
  public static double getNoDataValue(GridSampleDimension sampleDimension) {
    List<Category> categories = sampleDimension.getCategories();
    InternationalString noDataCategoryName = Category.NODATA.getName();
    for (Category category : categories) {
      if (category.getName().equals(noDataCategoryName)) {
        return category.getRange().getMinimum();
      }
    }
    return Double.NaN;
  }

  /**
   * Get a GDAL-compliant affine transform from the given raster, where the grid coordinate
   * indicates the upper left corner of the pixel. PostGIS also follows GDAL convention.
   *
   * @param raster The raster to get the affine transform from.
   * @return The affine transform.
   */
  public static AffineTransform2D getGDALAffineTransform(GridCoverage2D raster) {
    return getAffineTransform(raster, PixelOrientation.UPPER_LEFT);
  }

  public static AffineTransform2D getAffineTransform(
      GridCoverage2D raster, PixelOrientation orientation) throws UnsupportedOperationException {
    GridGeometry2D gridGeometry2D = raster.getGridGeometry();
    MathTransform crsTransform = gridGeometry2D.getGridToCRS2D(orientation);
    if (!(crsTransform instanceof AffineTransform2D)) {
      throw new UnsupportedOperationException("Only AffineTransform2D is supported");
    }
    return (AffineTransform2D) crsTransform;
  }

  /**
   * Translate an affine transformation by a given offset.
   *
   * @param affine the affine transformation
   * @param offsetX the offset in x direction
   * @param offsetY the offset in y direction
   * @return the translated affine transformation
   */
  public static AffineTransform2D translateAffineTransform(
      AffineTransform2D affine, int offsetX, int offsetY) {
    double ipX = affine.getTranslateX();
    double ipY = affine.getTranslateY();
    double scaleX = affine.getScaleX();
    double scaleY = affine.getScaleY();
    double skewX = affine.getShearX();
    double skewY = affine.getShearY();

    // Move the origin using the affine transformation, and leave scale and skew unchanged.
    double newIpX = ipX + offsetX * scaleX + offsetY * skewX;
    double newIpY = ipY + offsetX * skewY + offsetY * scaleY;
    return new AffineTransform2D(scaleX, skewY, skewX, scaleY, newIpX, newIpY);
  }

  public static Point2D getWorldCornerCoordinates(GridCoverage2D raster, int colX, int rowY)
      throws TransformException {
    Point2D.Double gridCoord = new Position2D(colX - 1, rowY - 1);
    return raster
        .getGridGeometry()
        .getGridToCRS2D(PixelOrientation.UPPER_LEFT)
        .transform(gridCoord, null);
  }

  /**
   * * Returns the world coordinates of the given grid coordinate. The expected grid coordinates are
   * 1 indexed. The function also enforces a range check to make sure given grid coordinates are
   * actually inside the grid.
   *
   * @param raster
   * @param colX
   * @param rowY
   * @return
   * @throws IndexOutOfBoundsException
   * @throws TransformException
   */
  public static Point2D getWorldCornerCoordinatesWithRangeCheck(
      GridCoverage2D raster, int colX, int rowY)
      throws IndexOutOfBoundsException, TransformException {
    Point2D.Double gridCoordinates2D = new Position2D(colX - 1, rowY - 1);
    if (!(raster.getGridGeometry().getGridRange2D().contains(gridCoordinates2D)))
      throw new IndexOutOfBoundsException(
          String.format(
              "Specified pixel coordinates (%d, %d) do not lie in the raster", colX, rowY));
    return raster
        .getGridGeometry()
        .getGridToCRS2D(PixelOrientation.UPPER_LEFT)
        .transform(gridCoordinates2D, null);
  }

  public static int[] getGridCoordinatesFromWorld(
      GridCoverage2D raster, double longitude, double latitude) throws TransformException {
    Point2D directPosition2D =
        new Position2D(raster.getCoordinateReferenceSystem2D(), longitude, latitude);
    Point2D worldCoord =
        raster
            .getGridGeometry()
            .getCRSToGrid2D(PixelOrientation.UPPER_LEFT)
            .transform(directPosition2D, null);
    double[] coords = new double[] {worldCoord.getX(), worldCoord.getY()};
    int[] gridCoords = new int[] {(int) Math.floor(coords[0]), (int) Math.floor(coords[1])};
    return gridCoords;
  }

  /**
   * * Throws an exception if band index is greater than the number of bands in a raster
   *
   * @param raster
   * @param band
   * @return
   * @throws IllegalArgumentException
   */
  public static void ensureBand(GridCoverage2D raster, int band) throws IllegalArgumentException {
    if (band < 1 || band > RasterAccessors.numBands(raster)) {
      throw new IllegalArgumentException(
          String.format("Provided band index %d is not present in the raster", band));
    }
  }

  public static Raster getRaster(RenderedImage renderedImage) {
    while (renderedImage instanceof RenderedImageAdapter) {
      renderedImage = ((RenderedImageAdapter) renderedImage).getWrappedImage();
    }
    if (renderedImage instanceof BufferedImage) {
      // This is a fast path for BufferedImage. If we call getData() directly, it will make a
      // hard copy of the raster. We can avoid this overhead by calling getRaster().
      return ((BufferedImage) renderedImage).getRaster();
    } else {
      return renderedImage.getData();
    }
  }

  public static Geometry convertCRSIfNeeded(
      Geometry geometry, CoordinateReferenceSystem targetCRS) {
    int geomSRID = geometry.getSRID();
    // If the geometry has a SRID, and it is not the same as the raster CRS, we need to transform
    // the geometry to the raster CRS.
    // Note that:
    // In Sedona vector, we do not perform implicit CRS transform. Everything must be done
    // explicitly via ST_Transform
    // In Sedona raster, we do implicit CRS transform if the raster has a CRS. If the SRID of
    // the geometry is not positive, we assume it is 4326.
    if (geomSRID <= 0) {
      geomSRID = 4326;
    }
    if (targetCRS != null && !(targetCRS instanceof DefaultEngineeringCRS)) {
      try {
        geometry =
            FunctionsGeoTools.transformToGivenTarget(geometry, "epsg:" + geomSRID, targetCRS, true);
      } catch (FactoryException | TransformException e) {
        throw new RuntimeException("Cannot transform CRS of query window", e);
      }
    }
    return geometry;
  }

  /**
   * Transforms a geometry into a raster's coordinate space. Missing raster CRS metadata is
   * interpreted as WGS84, matching the existing raster predicate policy.
   *
   * @param raster raster whose coordinate space is the target
   * @param geometry geometry to transform
   * @return geometry expressed in the raster's coordinate space
   */
  public static Geometry transformToRasterCRS(GridCoverage2D raster, Geometry geometry) {
    CoordinateReferenceSystem targetCRS = raster.getCoordinateReferenceSystem();
    if (targetCRS == null || targetCRS instanceof DefaultEngineeringCRS) {
      targetCRS = DefaultGeographicCRS.WGS84;
    }
    int geometrySRID = geometry.getSRID();
    if (geometrySRID <= 0) {
      geometrySRID = 4326;
    }
    if (RasterPredicates.isCRSMatchesSRID(targetCRS, geometrySRID)) {
      // Preserve the identifier-only fast path for the common matched-CRS case.
      return geometry;
    }
    return convertCRSIfNeeded(geometry, targetCRS);
  }

  /**
   * If the raster has a CRS, then it transforms the geom to the raster's CRS. If any of the inputs,
   * raster or geom doesn't have a CRS, it defaults to 4326.
   *
   * @param raster
   * @param geom
   * @return
   * @throws FactoryException
   */
  public static Pair<GridCoverage2D, Geometry> setDefaultCRSAndTransform(
      GridCoverage2D raster, Geometry geom) throws FactoryException {
    int rasterSRID = RasterAccessors.srid(raster);
    int geomSRID = Functions.getSRID(geom);

    if (rasterSRID == 0) {
      raster = RasterEditors.setSrid(raster, 4326);
      rasterSRID = RasterAccessors.srid(raster);
    }

    if (geomSRID == 0) {
      geom = Functions.setSRID(geom, 4326);
      geomSRID = Functions.getSRID(geom);
    }

    if (rasterSRID != geomSRID) {
      // implicitly converting roi geometry CRS to raster CRS
      geom = convertCRSIfNeeded(geom, raster.getCoordinateReferenceSystem());
      // have to set the SRID as RasterUtils.convertCRSIfNeeded doesn't set it even though the
      // geometry is in raster's CRS
      geom = Functions.setSRID(geom, RasterAccessors.srid(raster));
    }
    return Pair.of(raster, geom);
  }

  /**
   * Converts data types to data type codes
   *
   * @param s pixel data type possible values {D, I, B, F, S, US} <br>
   *     <br>
   *     Update: add support to convert RS_BandPixelType data type string to data type code possible
   *     values: <br>
   *     {REAL_64BITS, SIGNED_32BITS, UNSIGNED_8BITS, REAL_32BITS, SIGNED_16BITS, UNSIGNED_16BITS}
   * @return Data type code
   */
  public static int getDataTypeCode(String s) {
    switch (s.toUpperCase()) {
      case "D":
      case "REAL_64BITS":
        return 5;
      case "I":
      case "SIGNED_32BITS":
        return 3;
      case "B":
      case "UNSIGNED_8BITS":
        return 0;
      case "F":
      case "REAL_32BITS":
        return 4;
      case "S":
      case "SIGNED_16BITS":
        return 2;
      case "US":
      case "UNSIGNED_16BITS":
        return 1;
    }
    return 5; // defaulting to double
  }

  public static boolean isDataTypeIntegral(int dataTypeCode) {
    // returns true if the datatype code refers to an int-like datatype (int, short, etc.)
    switch (dataTypeCode) {
      case 3: // int
      case 0: // byte
      case 2: // short
      case 1: // unsigned short
        return true;
      case 5: // double
      case 4: // float
      default:
        return false;
    }
  }

  /**
   * Verifies that {@code noDataValue} can be stored in the pixel type named by {@code pixelType}
   * without silent coercion, returning it unchanged when it can. Delegates to {@link
   * #assertRepresentable(double, String, String)} with the argument name {@code "noDataValue"} so
   * the nodata path keeps its existing error messages.
   *
   * @param noDataValue the candidate nodata / background value
   * @param pixelType a Sedona pixel type string accepted by {@link #getDataTypeCode(String)} (for
   *     example {@code "B"}, {@code "I"}, {@code "D"})
   * @return {@code noDataValue}, unchanged, when it is representable in the pixel type
   * @throws IllegalArgumentException when {@code noDataValue} cannot be represented in the pixel
   *     type
   */
  public static double assertNoDataValueRepresentable(double noDataValue, String pixelType) {
    // A nodata value is a sentinel: it must be stored without any coercion (an exact 32-bit float
    // round-trip, and no negative-zero collapse on integer bands) so the stored background always
    // matches the recorded nodata metadata.
    return assertRepresentable(noDataValue, pixelType, "noDataValue", true);
  }

  /**
   * Verifies that a burn value can be stored in {@code pixelType}. Unlike a nodata sentinel, a burn
   * value is ordinary pixel data and does not have to round-trip exactly: a fractional value on a
   * float / double band (for example {@code 0.1} on {@code "F"}, stored as {@code 0.10000000149…})
   * is accepted and rounded to the pixel type, matching PostGIS {@code ST_AsRaster} and {@code
   * gdal_rasterize}. An out-of-range or fractional value on an integer band is still rejected,
   * since that would burn a different whole number than the caller asked for.
   *
   * @param value the candidate burn value
   * @param pixelType a Sedona pixel type string accepted by {@link #getDataTypeCode(String)}
   * @return {@code value}, unchanged, when it is storable in the pixel type
   * @throws IllegalArgumentException when {@code value} cannot be stored in the pixel type
   */
  public static double assertBurnValueRepresentable(double value, String pixelType) {
    return assertRepresentable(value, pixelType, "value", false);
  }

  /**
   * Verifies that {@code value} (a nodata / background value, or a burn value) can be stored in the
   * pixel type named by {@code pixelType} without silent coercion, returning it unchanged when it
   * can. Writing a value that is out of the pixel type's range, or fractional for an integer pixel
   * type, coerces the stored sample to a different number than the value the caller asked for (for
   * example -1.0 or 300.0 wraps to 255 or 44 in an unsigned 8-bit band), so the sample would read
   * back as a different number than requested. Any non-finite value (NaN or +/-Infinity) is always
   * rejected. When {@code requireExactStorage} is true the value is treated as a sentinel that must
   * round-trip byte-for-byte, so a float / double value that does not survive 32-bit float storage,
   * and negative zero on an integer band (whose sign an integer sample cannot preserve), are also
   * rejected; burn values pass false, since rounding to the pixel type is expected of ordinary
   * pixel data. Such a violation is a correctness error, not a per-row data condition, so this
   * rejects it with an {@link IllegalArgumentException} instead of coercing.
   *
   * @param value the candidate value
   * @param pixelType a Sedona pixel type string accepted by {@link #getDataTypeCode(String)} (for
   *     example {@code "B"}, {@code "I"}, {@code "D"})
   * @param argName the name of the argument being validated (for example {@code "noDataValue"} or
   *     {@code "value"}), used in the exception message
   * @param requireExactStorage true to require the value be stored without any coercion (a nodata
   *     sentinel); false to allow rounding to the pixel type (a burn value)
   * @return {@code value}, unchanged, when it is representable in the pixel type
   * @throws IllegalArgumentException when {@code value} cannot be represented in the pixel type
   */
  public static double assertRepresentable(
      double value, String pixelType, String argName, boolean requireExactStorage) {
    int dataTypeCode = getDataTypeCode(pixelType);
    long min;
    long max;
    String description;
    switch (dataTypeCode) {
      case DataBuffer.TYPE_BYTE:
        min = 0;
        max = 255;
        description = "unsigned 8-bit";
        break;
      case DataBuffer.TYPE_USHORT:
        min = 0;
        max = 65535;
        description = "unsigned 16-bit";
        break;
      case DataBuffer.TYPE_SHORT:
        min = Short.MIN_VALUE;
        max = Short.MAX_VALUE;
        description = "signed 16-bit";
        break;
      case DataBuffer.TYPE_INT:
        min = Integer.MIN_VALUE;
        max = Integer.MAX_VALUE;
        description = "signed 32-bit";
        break;
      case DataBuffer.TYPE_FLOAT:
        // NaN and infinite values cannot be used on a float/double band: NaN is the codebase's
        // internal "no nodata" sentinel, so it cannot be stored as a distinct nodata value and
        // would be silently dropped; an infinite value is not a valid GeoTools category bound and
        // crashes deep in GeoTools with "Range [Infinity .. Infinity] is not valid". Reject both
        // up front with a clear error (fail loud) rather than dropping them or crashing later.
        if (!Double.isFinite(value)) {
          throw new IllegalArgumentException(
              String.format(
                  "%s %s is not supported for pixel type '%s' (NaN and infinite values are not supported); use a finite value",
                  argName, value, pixelType));
        }
        // A nodata sentinel must round-trip exactly through the band's 32-bit float storage —
        // otherwise the stored sentinel differs from the value the caller set (e.g. 0.1 would be
        // stored as 0.10000000149...), and comparisons against the caller's value would miss it. A
        // burn value is ordinary data, so it is allowed to round to the pixel type (as PostGIS and
        // gdal_rasterize do) and skips this check.
        if (requireExactStorage && (double) (float) value != value) {
          throw new IllegalArgumentException(
              String.format(
                  "%s %s is not exactly representable in pixel type '%s' (32-bit float)",
                  argName, value, pixelType));
        }
        return value;
      case DataBuffer.TYPE_DOUBLE:
      default:
        // 64-bit float represents every finite double value; only NaN and infinities are rejected
        // (see the float arm for why).
        if (!Double.isFinite(value)) {
          throw new IllegalArgumentException(
              String.format(
                  "%s %s is not supported for pixel type '%s' (NaN and infinite values are not supported); use a finite value",
                  argName, value, pixelType));
        }
        return value;
    }

    // A nodata sentinel of negative zero cannot be stored in an integer sample: the sign is lost,
    // so the background reads back as +0 while the recorded nodata metadata is -0.0, leaving the
    // background counted as data (and B/US fail later constructing the category as "Ranges
    // overlap").
    // Ordinary comparisons treat -0.0 as +0.0, so it slips through the range/fractional check
    // below;
    // reject it explicitly for integer pixel types. +0.0 and float/double -0.0 are unaffected, and
    // burn values (requireExactStorage == false) are ordinary data, so -0.0 simply stores as 0.
    if (requireExactStorage
        && Double.doubleToRawLongBits(value) == Double.doubleToRawLongBits(-0.0d)) {
      throw new IllegalArgumentException(
          String.format(
              "%s %s is not representable in pixel type '%s' (%s integer): negative zero cannot be "
                  + "stored in an integer sample; use 0",
              argName, value, pixelType, description));
    }

    // Integer pixel types: reject out-of-range and fractional values (Math.rint also rejects NaN).
    if (value < min || value > max || value != Math.rint(value)) {
      throw new IllegalArgumentException(
          String.format(
              "%s %s is not representable in pixel type '%s' (%s integer, valid range %d..%d)",
              argName, value, pixelType, description, min, max));
    }
    return value;
  }

  /**
   * This is an experimental method as it does not copy the original raster properties (e.g. color
   * model, sample model, etc.) moved from MapAlgebra.java TODO: Copy the original raster properties
   *
   * @param gridCoverage2D
   * @param bandValues
   * @return
   */
  public static GridCoverage2D copyRasterAndAppendBand(
      GridCoverage2D gridCoverage2D, Object bandValues, Double noDataValue) {
    // Get the original image and its properties
    RenderedImage originalImage = gridCoverage2D.getRenderedImage();
    Raster raster = getRaster(originalImage);
    Point location = raster.getBounds().getLocation();
    WritableRaster wr =
        RasterFactory.createBandedRaster(
            raster.getDataBuffer().getDataType(),
            originalImage.getWidth(),
            originalImage.getHeight(),
            gridCoverage2D.getNumSampleDimensions() + 1,
            location);
    // Copy the raster data and append the new band values
    for (int i = 0; i < raster.getWidth(); i++) {
      for (int j = 0; j < raster.getHeight(); j++) {
        if (bandValues instanceof double[]) {
          double[] values = (double[]) bandValues;
          double[] pixels = raster.getPixel(i, j, (double[]) null);
          double[] copiedPixels = new double[pixels.length + 1];
          System.arraycopy(pixels, 0, copiedPixels, 0, pixels.length);
          copiedPixels[pixels.length] = values[j * raster.getWidth() + i];
          wr.setPixel(i, j, copiedPixels);
        } else if (bandValues instanceof int[]) {
          int[] values = (int[]) bandValues;
          int[] pixels = raster.getPixel(i, j, (int[]) null);
          int[] copiedPixels = new int[pixels.length + 1];
          System.arraycopy(pixels, 0, copiedPixels, 0, pixels.length);
          copiedPixels[pixels.length] = values[j * raster.getWidth() + i];
          wr.setPixel(i, j, copiedPixels);
        }
      }
    }
    // Add a sample dimension for newly added band
    int numBand = wr.getNumBands();
    GridSampleDimension[] originalSampleDimensions = gridCoverage2D.getSampleDimensions();
    GridSampleDimension[] sampleDimensions = new GridSampleDimension[numBand];
    System.arraycopy(
        originalSampleDimensions, 0, sampleDimensions, 0, originalSampleDimensions.length);
    if (noDataValue != null) {
      sampleDimensions[numBand - 1] =
          createSampleDimensionWithNoDataValue("band" + numBand, noDataValue);
    } else {
      sampleDimensions[numBand - 1] = new GridSampleDimension("band" + numBand);
    }
    // Construct a GridCoverage2D with the copied image.
    return clone(
        wr, gridCoverage2D.getGridGeometry(), sampleDimensions, gridCoverage2D, null, true);
  }

  public static GridCoverage2D copyRasterAndAppendBand(
      GridCoverage2D gridCoverage2D, Object bandValues) {
    return copyRasterAndAppendBand(gridCoverage2D, bandValues, null);
  }

  public static GridCoverage2D copyRasterAndReplaceBand(
      GridCoverage2D gridCoverage2D,
      int bandIndex,
      Object bandValues,
      Double noDataValue,
      boolean removeNoDataIfNull) {
    // Do not allow the band index to be out of bounds
    ensureBand(gridCoverage2D, bandIndex);
    // Get the original image and its properties
    RenderedImage originalImage = gridCoverage2D.getRenderedImage();
    Raster raster = getRaster(originalImage);
    WritableRaster wr = raster.createCompatibleWritableRaster();
    // Copy the raster data and replace the band values
    for (int i = 0; i < raster.getWidth(); i++) {
      for (int j = 0; j < raster.getHeight(); j++) {
        if (bandValues instanceof double[]) {
          double[] values = (double[]) bandValues;
          double[] bands = raster.getPixel(i, j, (double[]) null);
          bands[bandIndex - 1] = values[j * raster.getWidth() + i];
          wr.setPixel(i, j, bands);
        } else if (bandValues instanceof int[]) {
          int[] values = (int[]) bandValues;
          int[] bands = raster.getPixel(i, j, (int[]) null);
          bands[bandIndex - 1] = values[j * raster.getWidth() + i];
          wr.setPixel(i, j, bands);
        }
      }
    }
    GridSampleDimension[] sampleDimensions = gridCoverage2D.getSampleDimensions();
    GridSampleDimension sampleDimension = sampleDimensions[bandIndex - 1];
    if (noDataValue == null && removeNoDataIfNull) {
      sampleDimensions[bandIndex - 1] = removeNoDataValue(sampleDimension);
    } else if (noDataValue != null) {
      sampleDimensions[bandIndex - 1] =
          createSampleDimensionWithNoDataValue(sampleDimension, noDataValue);
    }
    return clone(
        wr, gridCoverage2D.getGridGeometry(), sampleDimensions, gridCoverage2D, null, true);
  }

  public static GridCoverage2D copyRasterAndReplaceBand(
      GridCoverage2D gridCoverage2D, int bandIndex, Object bandValues) {
    return copyRasterAndReplaceBand(gridCoverage2D, bandIndex, bandValues, null, false);
  }

  /**
   * Check if the two rasters are of the same shape
   *
   * @param raster1
   * @param raster2
   */
  public static void isRasterSameShape(GridCoverage2D raster1, GridCoverage2D raster2) {
    int width1 = RasterAccessors.getWidth(raster1), height1 = RasterAccessors.getHeight(raster1);
    int width2 = RasterAccessors.getWidth(raster2), height2 = RasterAccessors.getHeight(raster2);

    if (width1 != width2 && height1 != height2) {
      throw new IllegalArgumentException(
          String.format(
              "Provided rasters are not of same shape. \n"
                  + "First raster having width of %d and height of %d. \n"
                  + "Second raster having width of %d and height of %d",
              width1, height1, width2, height2));
    }
  }

  /**
   * Shift the rendered image inside the grid coverage to have a zero origin. This won't alter the
   * cell values of the raster, but will shift the affine transformation to cancel with the origin
   * shift.
   *
   * @param raster The raster to shift
   * @param noDataValue The no data value to use for the new raster
   * @return A new grid coverage with the same cell values but its rendered image has a zero origin
   */
  public static GridCoverage2D shiftRasterToZeroOrigin(GridCoverage2D raster, Double noDataValue) {
    RenderedImage image = raster.getRenderedImage();
    SampleModel sampleModel = image.getSampleModel();
    int width = image.getWidth();
    int height = image.getHeight();
    int minX = image.getMinX();
    int minY = image.getMinY();
    if (minX != 0 || minY != 0) {
      GridGeometry2D gridGeometry = raster.getGridGeometry();
      AffineTransform2D transform = (AffineTransform2D) gridGeometry.getGridToCRS2D();
      AffineTransform2D newAffine = RasterUtils.translateAffineTransform(transform, minX, minY);
      GridEnvelope newGridEnvelope = new GridEnvelope2D(0, 0, width, height);
      GridGeometry2D newGridGeometry =
          new GridGeometry2D(
              newGridEnvelope, newAffine, gridGeometry.getCoordinateReferenceSystem());
      WritableRaster wr =
          RasterFactory.createBandedRaster(
              sampleModel.getDataType(),
              image.getWidth(),
              image.getHeight(),
              sampleModel.getNumBands(),
              null);
      wr.setRect(-minX, -minY, RasterUtils.getRaster(image));
      return RasterUtils.clone(
          wr, newGridGeometry, raster.getSampleDimensions(), raster, noDataValue, false);
    } else {
      // Copy the data out to break the dependency on the original raster
      WritableRaster wr =
          RasterFactory.createBandedRaster(
              sampleModel.getDataType(),
              image.getWidth(),
              image.getHeight(),
              sampleModel.getNumBands(),
              null);
      wr.setRect(0, 0, RasterUtils.getRaster(image));
      return RasterUtils.clone(
          wr, raster.getGridGeometry(), raster.getSampleDimensions(), raster, noDataValue, false);
    }
  }

  /**
   * Retrieves a List<Double> of neighboring pixel values excluding noDataValue neighbors
   *
   * @param x grid coordinate
   * @param y grid coordinate
   * @param band Band index of raster
   * @param raster
   * @param noDataValue
   * @return
   */
  public static List<Double> getNeighboringPixels(
      int x, int y, int band, Raster raster, Double noDataValue) {
    List<Double> neighbors = new ArrayList<>();
    int width = raster.getWidth();
    int height = raster.getHeight();

    for (int dx = -1; dx <= 1; dx++) {
      for (int dy = -1; dy <= 1; dy++) {
        int nx = x + dx;
        int ny = y + dy;

        if (nx >= 0 && nx < width && ny >= 0 && ny < height && !(dx == 0 && dy == 0)) {
          double value = raster.getSampleDouble(nx, ny, band); // Now checks the specified band
          if (value != noDataValue) {
            neighbors.add(value);
          }
        }
      }
    }
    return neighbors;
  }

  /**
   * Replaces noDataValue pixels in each band with mean of neighboring pixel values
   *
   * @param raster
   * @return A new grid coverage with noDataValues pixels replaced
   */
  public static GridCoverage2D replaceNoDataValues(GridCoverage2D raster) {
    Raster rasterData = raster.getRenderedImage().getData();
    WritableRaster writableRaster = rasterData.createCompatibleWritableRaster();

    Median medianCalculator = new Median();

    // Iterate over each band
    for (int band = 0; band < raster.getNumSampleDimensions(); band++) {
      GridSampleDimension sampleDimension = raster.getSampleDimension(band);
      double noDataValue = RasterUtils.getNoDataValue(sampleDimension);

      // Replace no data values with the median of neighboring pixels for each band
      for (int y = 0; y < rasterData.getHeight(); y++) {
        for (int x = 0; x < rasterData.getWidth(); x++) {
          double originalValue = rasterData.getSampleDouble(x, y, band);
          if (originalValue == noDataValue) {
            List<Double> neighbors =
                RasterUtils.getNeighboringPixels(x, y, band, rasterData, noDataValue);
            double[] neighborArray = neighbors.stream().mapToDouble(Double::doubleValue).toArray();
            double medianValue =
                neighborArray.length > 0 ? medianCalculator.evaluate(neighborArray) : Double.NaN;
            writableRaster.setSample(
                x, y, band, !Double.isNaN(medianValue) ? medianValue : originalValue);
          } else {
            writableRaster.setSample(x, y, band, originalValue);
          }
        }
      }
    }

    GridCoverage2D modifiedRaster =
        RasterUtils.clone(
            writableRaster,
            raster.getGridGeometry(),
            raster.getSampleDimensions(),
            raster,
            null,
            true);
    return modifiedRaster;
  }

  /**
   * Filters out the noDataValue pixels from each band as a grid coverage
   *
   * @param raster
   * @return Returns a grid coverage with noDataValues and valid data values as Double.Nan
   */
  public static GridCoverage2D extractNoDataValueMask(GridCoverage2D raster) {
    Raster rasterData = raster.getRenderedImage().getData();
    WritableRaster writableRaster =
        rasterData.createCompatibleWritableRaster(
            RasterAccessors.getWidth(raster), RasterAccessors.getHeight(raster));

    // Iterate over each band
    for (int band = 0; band < raster.getNumSampleDimensions(); band++) {
      GridSampleDimension sampleDimension = raster.getSampleDimension(band);
      Double noDataValue = RasterUtils.getNoDataValue(sampleDimension);

      for (int y = 0; y < rasterData.getHeight(); y++) {
        for (int x = 0; x < rasterData.getWidth(); x++) {
          double originalValue = rasterData.getSampleDouble(x, y, band);
          if (originalValue == noDataValue) {
            writableRaster.setSample(x, y, band, originalValue);
          } else {
            writableRaster.setSample(x, y, band, Double.NaN);
          }
        }
      }
    }

    GridCoverage2D modifiedRaster =
        RasterUtils.clone(
            writableRaster,
            raster.getGridGeometry(),
            raster.getSampleDimensions(),
            raster,
            null,
            true);
    return modifiedRaster;
  }

  /**
   * Superimposes the mask values onto the original raster, maintaining the original values where
   * the mask is NaN.
   *
   * @param raster The original raster to which the mask will be applied.
   * @param mask Grid coverage mask to be applied, containing the values to overlay. This mask
   *     should have the same dimensions and number of bands as the original raster.
   * @return A new GridCoverage2D object with the mask applied.
   */
  public static GridCoverage2D applyRasterMask(GridCoverage2D raster, GridCoverage2D mask) {
    Raster rasterData = raster.getRenderedImage().getData();
    Raster maskData = mask.getRenderedImage().getData();
    WritableRaster writableRaster =
        rasterData.createCompatibleWritableRaster(
            RasterAccessors.getWidth(raster), RasterAccessors.getHeight(raster));

    // Iterate over each band
    for (int band = 0; band < raster.getNumSampleDimensions(); band++) {
      for (int y = 0; y < rasterData.getHeight(); y++) {
        for (int x = 0; x < rasterData.getWidth(); x++) {
          double originalValue = rasterData.getSampleDouble(x, y, band);
          double maskValue = maskData.getSampleDouble(x, y, band);
          if (!Double.isNaN(maskValue)) {
            writableRaster.setSample(x, y, band, maskValue);
          } else {
            writableRaster.setSample(x, y, band, originalValue);
          }
        }
      }
    }

    GridCoverage2D modifiedRaster =
        RasterUtils.clone(
            writableRaster,
            raster.getGridGeometry(),
            raster.getSampleDimensions(),
            raster,
            null,
            false);
    return modifiedRaster;
  }

  /**
   * This function calculates the upper-left Y world coordinate of the raster. It accounts for both
   * bottom-up and top-down raster conventions.
   *
   * <p>For bottom-up rasters (metadata[5] > 0), the upper-left Y world coordinate is directly taken
   * from metadata[1]. - For top-down rasters (metadata[5] < 0), the upper-left Y coordinate is
   * adjusted by adding the product of the raster height and the scale factor (metadata[5]).
   *
   * @param metadata The metadata array containing raster properties.
   * @return The upper-left Y world coordinate of the raster.
   */
  public static double getRasterUpperLeftY(double[] metadata) {
    if (metadata[5] < 0) {
      return metadata[1];
    } else {
      int rasterHeight = (int) metadata[3];
      return metadata[1] + (rasterHeight * metadata[5]);
    }
  }

  /**
   * This function calculates the Y scale factor of the raster. It ensures that the scale factor is
   * always negative, regardless of the raster convention.
   *
   * <p>For bottom-up rasters (metadata[5] > 0), the scale factor is returned as-is. - For top-down
   * rasters (metadata[5] < 0), the scale factor is negated to maintain consistency.
   *
   * @param metadata The metadata array containing raster properties.
   * @return The Y scale factor of the raster.
   */
  public static double getRasterScaleY(double[] metadata) {
    if (metadata[5] < 0) {
      return metadata[5];
    } else {
      return -metadata[5];
    }
  }

  /**
   * GRID SPACE: Grid space refers to the raster's index space (col, row). Pixel values remain at
   * the same array indices, but the grid-to-CRS affine transform is rewritten.
   *
   * <p>EFFECT: - The raster is reinterpreted spatially (top-down ↔ bottom-up). - Pixel values are
   * NOT moved in memory. - Pixel world coordinates DO change. - The world envelope (bounding box)
   * is preserved.
   *
   * <p>Use this when you want to change how the raster is oriented in CRS space without touching
   * the pixel data.
   */
  public static GridCoverage2D flipVerticallyGridSpace(GridCoverage2D raster) {

    GridGeometry2D gridGeometry = raster.getGridGeometry();

    MathTransform mt = gridGeometry.getGridToCRS2D();
    if (!(mt instanceof AffineTransform2D)) {
      throw new UnsupportedOperationException(
          "flipVertically only supports AffineTransform2D grid-to-CRS transforms");
    }

    AffineTransform2D affine = (AffineTransform2D) mt;

    RenderedImage image = raster.getRenderedImage();
    int height = image.getHeight();

    /*
     * Affine transform definition (GeoTools / GDAL compatible):
     *
     *   worldX = scaleX * col + shearX * row + translateX
     *   worldY = shearY * col + scaleY * row + translateY
     */
    double scaleX = affine.getScaleX();
    double shearX = affine.getShearX();
    double shearY = affine.getShearY();
    double scaleY = affine.getScaleY();
    double translateX = affine.getTranslateX();
    double translateY = affine.getTranslateY();

    /*
     * Apply grid-space vertical flip:
     *
     *   row = (H - 1) - row'
     */
    double newShearX = -shearX;
    double newScaleY = -scaleY;

    double newTranslateX = translateX + shearX * (height - 1);
    double newTranslateY = translateY + scaleY * (height - 1);

    AffineTransform2D flippedAffine =
        new AffineTransform2D(scaleX, shearY, newShearX, newScaleY, newTranslateX, newTranslateY);

    GridGeometry2D newGridGeometry =
        new GridGeometry2D(
            gridGeometry.getGridRange(),
            flippedAffine,
            gridGeometry.getCoordinateReferenceSystem());

    return RasterUtils.clone(
        raster.getRenderedImage(),
        newGridGeometry,
        raster.getSampleDimensions(),
        raster,
        null,
        true);
  }

  /**
   * PIXEL SPACE: Pixel space refers to pixel values anchored at their world coordinates. Pixel
   * values are physically rearranged so that each value remains at the same CRS location.
   *
   * <p>EFFECT: - Raster rows are reversed in memory. - The affine transform is updated accordingly.
   * - Pixel world coordinates are preserved. - The world envelope (bounding box) is preserved.
   *
   * <p>Use this when pixel values must remain fixed at their geographic locations.
   */
  public static GridCoverage2D flipVerticallyPixelSpace(GridCoverage2D raster) {
    GridGeometry2D gridGeometry = raster.getGridGeometry();

    MathTransform mt = gridGeometry.getGridToCRS2D();
    if (!(mt instanceof AffineTransform2D)) {
      throw new UnsupportedOperationException(
          "flipVertically only supports AffineTransform2D grid-to-CRS transforms");
    }

    AffineTransform2D affine = (AffineTransform2D) mt;

    RenderedImage image = raster.getRenderedImage();
    int height = image.getHeight();

    // Flip the affine transformation
    double scaleX = affine.getScaleX();
    double shearX = affine.getShearX();
    double shearY = affine.getShearY();
    double scaleY = affine.getScaleY();
    double translateX = affine.getTranslateX();
    double translateY = affine.getTranslateY();

    double newShearX = -shearX;
    double newScaleY = -scaleY;
    double newTranslateX = translateX + shearX * (height - 1);
    double newTranslateY = translateY + scaleY * (height - 1);

    AffineTransform2D flippedAffine =
        new AffineTransform2D(scaleX, shearY, newShearX, newScaleY, newTranslateX, newTranslateY);

    // Create a new WritableRaster to preserve pixel values
    Raster originalRaster = RasterUtils.getRaster(image);
    WritableRaster flippedRaster = originalRaster.createCompatibleWritableRaster();

    // Copy pixel values to the flipped raster
    for (int y = 0; y < height; y++) {
      for (int x = 0; x < image.getWidth(); x++) {
        flippedRaster.setPixel(x, height - y - 1, originalRaster.getPixel(x, y, (double[]) null));
      }
    }

    GridGeometry2D newGridGeometry =
        new GridGeometry2D(
            gridGeometry.getGridRange(),
            flippedAffine,
            gridGeometry.getCoordinateReferenceSystem());

    return RasterUtils.clone(
        flippedRaster, newGridGeometry, raster.getSampleDimensions(), raster, null, true);
  }
}
