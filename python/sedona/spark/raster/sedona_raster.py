# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from abc import ABC, abstractmethod
from typing import List, Optional, Sequence, Union
import json
from xml.etree.ElementTree import Element, SubElement, tostring  # nosec B405

import numpy as np
import rasterio  # type: ignore
import rasterio.env  # type: ignore
from rasterio.io import DatasetReader  # type: ignore
from rasterio.io import MemoryFile  # type: ignore
from rasterio.transform import Affine  # type: ignore

try:
    # for rasterio >= 1.3.0
    from rasterio._path import _parse_path as parse_path  # type: ignore
except:
    # for rasterio >= 1.2.0
    from rasterio.path import parse_path  # type: ignore

from .awt_raster import AWTRaster
from .data_buffer import DataBuffer
from .meta import AffineTransform, SampleDimension
from .sample_model import (
    ComponentSampleModel,
    MultiPixelPackedSampleModel,
    SampleModel,
    SinglePixelPackedSampleModel,
)

GDAL_VERSION = rasterio.env.GDALVersion.runtime()

_SAMPLE_DIMENSION_NO_DATA_VALUE_OVERRIDE = 0x01
_SAMPLE_DIMENSION_SAMPLE_TYPE_OVERRIDE = 0x02


def _has_env_with_gdal_mem_enabled():
    if rasterio.env.hasenv():
        if GDAL_VERSION.at_least(rasterio.env.GDALVersion(3, 10)):
            # For GDAL >= 3.10, GDAL_MEM_ENABLE_OPEN must be enabled to load
            # MEM:: dataset. Please refer to
            # https://gdal.org/en/latest/drivers/raster/mem.html for details.
            options = rasterio.env.getenv()
            return options.get("GDAL_MEM_ENABLE_OPEN") == "YES"
        else:
            return True
    else:
        return False


def _rasterio_open(fp, driver=None):
    """A variant of rasterio.open. This function skip setting up a new GDAL env
    when there is already an environment. This saves us lots of overhead
    introduced by GDAL env initialization.

    """
    if _has_env_with_gdal_mem_enabled():
        # There is already an env, so we can get rid of the overhead of
        # GDAL env initialization in rasterio.open().
        return DatasetReader(parse_path(fp), driver=driver)
    else:
        with rasterio.env.Env(GDAL_MEM_ENABLE_OPEN="YES"):
            return rasterio.open(fp, mode="r", driver=driver)


def _rasterio_open_memfile(memfile: MemoryFile, driver=None):
    if not _has_env_with_gdal_mem_enabled():
        with rasterio.env.Env(GDAL_MEM_ENABLE_OPEN="YES"):
            return memfile.open(driver=driver)
    return memfile.open(driver=driver)


def _numpy_dtype_to_data_buffer_type(dtype: np.dtype) -> int:
    """Map a numpy dtype to the corresponding DataBuffer type constant.

    Parameters
    ----------
    dtype : np.dtype
        The numpy dtype to map.

    Returns
    -------
    int
        One of DataBuffer.TYPE_* constants.

    Raises
    ------
    ValueError
        If the dtype has no corresponding DataBuffer type.
    """
    dtype = np.dtype(dtype)  # normalize
    mapping = {
        np.dtype(np.uint8): DataBuffer.TYPE_BYTE,
        np.dtype(np.int8): DataBuffer.TYPE_BYTE,
        np.dtype(np.int16): DataBuffer.TYPE_SHORT,
        np.dtype(np.uint16): DataBuffer.TYPE_USHORT,
        np.dtype(np.int32): DataBuffer.TYPE_INT,
        np.dtype(np.uint32): DataBuffer.TYPE_INT,
        np.dtype(np.float32): DataBuffer.TYPE_FLOAT,
        np.dtype(np.float64): DataBuffer.TYPE_DOUBLE,
    }
    if dtype not in mapping:
        raise ValueError(
            f"Unsupported numpy dtype {dtype} for raster serialization. "
            f"Supported: uint8, int8, int16, uint16, int32, uint32, float32, float64. "
            f"Note: uint32 maps to signed TYPE_INT — values above 2^31-1 will "
            f"overflow silently. int8 maps to TYPE_BYTE (unsigned on JVM) — "
            f"negative values will be reinterpreted."
        )
    return mapping[dtype]


def _sample_model_band_size(sample_model: SampleModel, band: int) -> int:
    """Return the number of bits Java exposes for one band of a SampleModel."""
    storage_sizes = {
        DataBuffer.TYPE_BYTE: 8,
        DataBuffer.TYPE_USHORT: 16,
        DataBuffer.TYPE_SHORT: 16,
        DataBuffer.TYPE_INT: 32,
        DataBuffer.TYPE_FLOAT: 32,
        DataBuffer.TYPE_DOUBLE: 64,
    }
    storage_size = storage_sizes[sample_model.data_type]
    if isinstance(sample_model, SinglePixelPackedSampleModel):
        source_band = min(band, len(sample_model.bit_masks) - 1)
        mask = sample_model.bit_masks[source_band] & ((1 << storage_size) - 1)
        return bin(mask).count("1")
    if isinstance(sample_model, MultiPixelPackedSampleModel):
        return sample_model.num_bits
    return storage_size


def _generate_vrt_xml(
    src_path,
    data_type,
    width,
    height,
    geo_transform,
    crs_wkt,
    off_x,
    off_y,
    band_indices,
) -> bytes:
    # Create root element
    root = Element("VRTDataset")
    root.set("rasterXSize", str(width))
    root.set("rasterYSize", str(height))

    # Add CRS
    if crs_wkt is not None and crs_wkt != "":
        srs = SubElement(root, "SRS")
        srs.text = crs_wkt

    # Add GeoTransform
    gt = SubElement(root, "GeoTransform")
    gt.text = geo_transform

    # Add bands
    for i, band_index in enumerate(band_indices, start=1):
        band = SubElement(root, "VRTRasterBand")
        band.set("dataType", data_type)
        band.set("band", str(i))

        # Add source
        source = SubElement(band, "SimpleSource")
        src_prop = SubElement(source, "SourceFilename")
        src_prop.text = src_path

        # Set source properties
        SubElement(source, "SourceBand").text = str(band_index + 1)
        SubElement(
            source,
            "SrcRect",
            {
                "xOff": str(off_x),
                "yOff": str(off_y),
                "xSize": str(width),
                "ySize": str(height),
            },
        )
        SubElement(
            source,
            "DstRect",
            {"xOff": "0", "yOff": "0", "xSize": str(width), "ySize": str(height)},
        )

    # Generate pretty XML
    xml_bytes = tostring(root, encoding="utf-8")
    return xml_bytes


class SedonaRaster(ABC):
    _width: int
    _height: int
    _bands_meta: List[SampleDimension]
    _affine_trans: AffineTransform
    _crs_wkt: str

    def __init__(
        self,
        width: int,
        height: int,
        bands_meta: List[SampleDimension],
        affine_trans: AffineTransform,
        crs_wkt: str,
    ):
        self._width = width
        self._height = height
        self._bands_meta = bands_meta
        self._affine_trans = affine_trans
        self._crs_wkt = crs_wkt

    @property
    def width(self) -> int:
        """Width of the raster in pixel"""
        return self._width

    @property
    def height(self) -> int:
        """Height of the raster in pixel"""
        return self._height

    @property
    def crs_wkt(self) -> str:
        """CRS of the raster as a WKT string"""
        return self._crs_wkt

    @property
    def bands_meta(self) -> List[SampleDimension]:
        """Metadata of bands, including nodata value for each band"""
        return self._bands_meta

    @property
    def affine_trans(self) -> AffineTransform:
        """Geo transform of the raster"""
        return self._affine_trans

    @abstractmethod
    def as_numpy(self) -> np.ndarray:
        """Get the bands data as an numpy array in CHW layout"""
        raise NotImplementedError()

    def as_numpy_masked(self) -> np.ndarray:
        """Get the bands data as an numpy array in CHW layout, with nodata
        values masked as nan.

        """
        arr = self.as_numpy()
        # Java stores int8 pixels in an unsigned byte buffer and uint32 pixels in a signed
        # int buffer. with_bands() keeps the caller's NumPy dtype until serialization while
        # bands_meta carries the Java-facing NODATA value, so compare through a storage view.
        # Keep arr for the result so valid samples retain their original signedness.
        storage_arr = arr
        if arr.dtype == np.dtype(np.int8):
            storage_arr = arr.view(np.uint8)
        elif arr.dtype == np.dtype(np.uint32):
            storage_arr = arr.view(np.int32)
        nodata_values = np.array([bm.nodata for bm in self._bands_meta])
        nodata_values_reshaped = nodata_values[:, None, None]
        mask = storage_arr == nodata_values_reshaped
        masked_arr = np.where(mask, np.nan, arr)
        return masked_arr

    @abstractmethod
    def as_rasterio(self) -> DatasetReader:
        """Retrieve the raster as an rasterio DatasetReader"""
        raise NotImplementedError()

    @abstractmethod
    def close(self):
        """Release all resources allocated for this sedona raster. The rasterio
        DatasetReader returned by as_rasterio() will also be closed.

        """
        raise NotImplementedError()

    def with_bands(
        self,
        new_data: np.ndarray,
        nodata: Optional[Union[float, Sequence[float]]] = None,
    ) -> "SedonaRaster":
        """Replace pixel data, preserving spatial metadata.

        Only supported on InDbSedonaRaster.
        """
        raise TypeError(f"with_bands() is not supported on {type(self).__name__}.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        self.close()


class InDbSedonaRaster(SedonaRaster):
    awt_raster: AWTRaster
    rasterio_memfile: Optional[MemoryFile]
    rasterio_dataset_reader: Optional[DatasetReader]

    def __init__(
        self,
        width: int,
        height: int,
        bands_meta: List[SampleDimension],
        affine_trans: AffineTransform,
        crs_wkt: str,
        awt_raster: AWTRaster,
    ):
        super().__init__(width, height, bands_meta, affine_trans, crs_wkt)
        self.awt_raster = awt_raster
        self.rasterio_memfile = None
        self.rasterio_dataset_reader = None

        # Cached opaque blobs for round-trip serialization.
        # These are set by raster_serde._deserialize() or with_bands().
        # If None, the raster cannot be serialized.
        self._name: str = ""
        self._category_blobs: Optional[List[bytes]] = None
        # An optional wire trailer marks why a band's Python output metadata must override
        # replayed source categories: bit 0 for NODATA and bit 1 for sample type.
        self._sample_dimension_override_flags: List[int] = [0] * len(bands_meta)
        self._properties_blob: Optional[bytes] = None
        self._color_model_blob: Optional[bytes] = None

    def as_numpy(self) -> np.ndarray:
        sm = self.awt_raster.sample_model
        return sm.as_numpy(self.awt_raster.data_buffer)

    def as_rasterio(self) -> DatasetReader:
        if self.rasterio_dataset_reader is not None:
            return self.rasterio_dataset_reader

        affine = Affine.from_gdal(
            self._affine_trans.ip_x,
            self._affine_trans.scale_x,
            self._affine_trans.skew_x,
            self._affine_trans.ip_y,
            self._affine_trans.skew_y,
            self._affine_trans.scale_y,
        )
        num_bands = len(self._bands_meta)

        data_array = np.ascontiguousarray(self.as_numpy())

        dtype = data_array.dtype
        if dtype == np.uint8:
            data_type = "Byte"
        elif dtype == np.int8:
            data_type = "Int8"
        elif dtype == np.uint16:
            data_type = "Uint16"
        elif dtype == np.int16:
            data_type = "Int16"
        elif dtype == np.uint32:
            data_type = "UInt32"
        elif dtype == np.int32:
            data_type = "Int32"
        elif dtype == np.float32:
            data_type = "Float32"
        elif dtype == np.float64:
            data_type = "Float64"
        elif dtype == np.int64:
            data_type = "Int64"
        elif dtype == np.uint64:
            data_type = "Uint64"
        else:
            raise RuntimeError("unknown dtype: " + str(dtype))

        arr_if = data_array.__array_interface__
        data_pointer = arr_if["data"][0]
        geotransform = (
            f"{self._affine_trans.ip_x}/{self._affine_trans.scale_x}/{self._affine_trans.skew_x}/"
            + f"{self._affine_trans.ip_y}/{self._affine_trans.skew_y}/{self._affine_trans.scale_y}"
        )
        desc = (
            f"MEM:::DATAPOINTER={data_pointer},PIXELS={self._width},LINES={self._height},BANDS={num_bands},"
            + f"DATATYPE={data_type},GEOTRANSFORM={geotransform}"
        )

        # If we are using GDAL >= 3.7, we can use the SPATIALREFERENCE
        # parameter; otherwise we have to wrap the MEM dataset with an VRT to
        # set up the SRS.
        if GDAL_VERSION.at_least(rasterio.env.GDALVersion(3, 7)):
            escaped_srs = json.dumps(self._crs_wkt.replace("\n", ""))
            desc += f",SPATIALREFERENCE={escaped_srs}"
            dataset = _rasterio_open(desc, driver="MEM")
        else:
            # construct a VRT to wrap this MEM dataset, with SRS set up properly
            vrt_xml = _generate_vrt_xml(
                desc,
                data_type,
                self._width,
                self._height,
                geotransform.replace("/", ","),
                self._crs_wkt,
                0,
                0,
                list(range(num_bands)),
            )
            self.rasterio_memfile = MemoryFile(vrt_xml, ext=".vrt")
            dataset = _rasterio_open_memfile(self.rasterio_memfile, driver="VRT")

        # XXX: dataset does not copy the data held by data_array, so we set
        # data_array as a property of dataset to make sure that the lifetime of
        # data_array is as long as dataset, otherwise we may see band data
        # corruption.
        dataset.mem_data_array = data_array
        return dataset

    def with_bands(
        self,
        new_data: np.ndarray,
        nodata: Optional[Union[float, Sequence[float]]] = None,
    ) -> "InDbSedonaRaster":
        """Create a new InDbSedonaRaster with replaced pixel data but same spatial metadata.

        The spatial metadata (CRS, affine transform, name) and cached opaque blobs
        (colorModel, properties) are preserved from the source raster. The category
        blobs and band metadata are adjusted to match the new band count.

        The colorModel blob is replayed unchanged even if band count or dtype changes.
        The JVM tolerates this: all analytical operations use the SampleModel (which
        is rebuilt correctly), and self-healing mechanisms in RasterEditors, MapAlgebra,
        and RasterUtils rebuild the colorModel when needed for rendering operations.

        The returned raster uses BandedSampleModel (BSQ layout) regardless of the
        source raster's SampleModel type. The JVM accepts any valid SampleModel
        during deserialization.

        Parameters
        ----------
        new_data : np.ndarray
            New pixel data. Accepted shapes:
              - (height, width) — interpreted as single-band CHW with C=1
              - (bands, height, width) — CHW layout
            Height and width must match the source raster.
            Band count and dtype may differ from source.
        nodata : float or sequence of float, optional
            NODATA value for the output bands. A scalar applies to every band; a
            sequence must have one entry per output band. Use ``float('nan')`` for
            a band that should have no NODATA.

            When omitted, each output band inherits NODATA from the source band in
            the same position, and bands beyond the source's band count inherit it
            from the source's last band. Pass this whenever the output means
            something different from the input — a 0/1 mask derived from a scene
            whose band has NODATA ``0`` would otherwise treat every unset pixel as
            NODATA.

        Returns
        -------
        InDbSedonaRaster
            A new raster with the given pixel data and adjusted metadata.

        Raises
        ------
        ValueError
            If spatial dimensions don't match, or if ``nodata`` is a sequence whose
            length differs from the output band count.
        RuntimeError
            If the source raster has no cached blobs (cannot be serialized).
        """
        if (
            self._category_blobs is None
            or self._properties_blob is None
            or self._color_model_blob is None
        ):
            raise RuntimeError(
                "Cannot call with_bands() on a raster without cached blob fields. "
                "Only rasters created by raster_serde.deserialize() support "
                "with_bands()."
            )

        if new_data.ndim == 2:
            new_data = new_data[np.newaxis, :, :]  # HW → CHW

        if new_data.ndim != 3:
            raise ValueError(
                f"new_data must be 2D (H, W) or 3D (C, H, W), got {new_data.ndim}D"
            )

        n_bands, h, w = new_data.shape
        if h != self._height or w != self._width:
            raise ValueError(
                f"Spatial dimensions ({h}, {w}) don't match raster "
                f"({self._height}, {self._width})"
            )

        # Map numpy dtype → DataBuffer type
        data_type = _numpy_dtype_to_data_buffer_type(new_data.dtype)

        # Adjust category blobs for new band count
        source_n_bands = len(self._bands_meta)
        output_sample_size = np.dtype(new_data.dtype).itemsize * 8
        source_sample_model = self.awt_raster.sample_model
        sample_type_changed = [
            data_type != source_sample_model.data_type
            or output_sample_size
            != _sample_model_band_size(
                source_sample_model, min(band, source_n_bands - 1)
            )
            for band in range(n_bands)
        ]
        if n_bands <= source_n_bands:
            category_blobs = list(self._category_blobs[:n_bands])
            metadata_override_flags = list(
                self._sample_dimension_override_flags[:n_bands]
            )
        else:
            category_blobs = list(self._category_blobs)
            metadata_override_flags = list(self._sample_dimension_override_flags)
            # Replicate last source category blob for new bands
            last_blob = self._category_blobs[-1]
            last_metadata_override_flags = self._sample_dimension_override_flags[-1]
            for _ in range(n_bands - source_n_bands):
                category_blobs.append(last_blob)
                metadata_override_flags.append(last_metadata_override_flags)

        # Adjust band metadata for new band count. Bands beyond the source's band count
        # replay the source's last category blob (above), so they inherit its nodata on
        # the JVM side — record that here rather than NaN, otherwise bands_meta and
        # RS_BandNoDataValue disagree about the same band.
        if n_bands <= source_n_bands:
            bands_meta = list(self._bands_meta[:n_bands])
        else:
            bands_meta = list(self._bands_meta)
            last_meta = self._bands_meta[-1]
            for _ in range(n_bands - source_n_bands):
                bands_meta.append(
                    SampleDimension(
                        description="",
                        offset=0.0,
                        scale=1.0,
                        nodata=last_meta.nodata,
                    )
                )

        if nodata is not None:
            bands_meta = self._override_nodata(bands_meta, nodata, new_data.dtype)
            metadata_override_flags = [
                flags
                | _SAMPLE_DIMENSION_NO_DATA_VALUE_OVERRIDE
                | (_SAMPLE_DIMENSION_SAMPLE_TYPE_OVERRIDE if type_changed else 0)
                for flags, type_changed in zip(
                    metadata_override_flags, sample_type_changed
                )
            ]
        else:
            inherited_meta = bands_meta
            bands_meta = self._normalize_inherited_nodata(
                inherited_meta, new_data.dtype
            )
            metadata_override_flags = [
                flags
                | (_SAMPLE_DIMENSION_SAMPLE_TYPE_OVERRIDE if type_changed else 0)
                | (
                    _SAMPLE_DIMENSION_NO_DATA_VALUE_OVERRIDE
                    if (
                        not (np.isnan(before.nodata) and np.isnan(after.nodata))
                        and before.nodata != after.nodata
                    )
                    else 0
                )
                for before, after, flags, type_changed in zip(
                    inherited_meta,
                    bands_meta,
                    metadata_override_flags,
                    sample_type_changed,
                )
            ]

        # Build BandedSampleModel (TYPE_BANDED = 1)
        # ComponentSampleModel.__init__() sets TYPE_COMPONENT, so we must
        # override to TYPE_BANDED after construction.
        new_sample_model = ComponentSampleModel(
            data_type,
            w,
            h,
            1,  # pixel_stride
            w,  # scanline_stride
            list(range(n_bands)),  # bank_indices: [0, 1, ..., n-1]
            [0] * n_bands,  # band_offsets: [0, 0, ..., 0]
        )
        new_sample_model.sample_model_type = SampleModel.TYPE_BANDED

        # Build DataBuffer: one bank per band, each flattened row-major
        banks = [np.ascontiguousarray(new_data[i].flatten()) for i in range(n_bands)]
        new_data_buffer = DataBuffer(data_type, banks, w * h, [0] * n_bands)

        new_awt_raster = AWTRaster(0, 0, w, h, new_sample_model, new_data_buffer)

        result = InDbSedonaRaster(
            self._width,
            self._height,
            bands_meta,
            self._affine_trans,
            self._crs_wkt,
            new_awt_raster,
        )
        result._name = self._name
        result._category_blobs = category_blobs
        result._sample_dimension_override_flags = metadata_override_flags
        result._properties_blob = self._properties_blob
        result._color_model_blob = self._color_model_blob  # replay unchanged
        return result

    @staticmethod
    def _as_nodata_scalar(value) -> Optional[float]:
        """Return value as a float if it is a real scalar, else None.

        np.float32(-9999) and friends are not Python floats but are scalars, so they
        have to be recognised here rather than falling through to the sequence branch.
        Complex numbers are rejected rather than silently losing their imaginary part,
        and bool is rejected because True as a NODATA value is almost certainly a
        mistake. 0-d arrays are unwrapped, since they are scalars that happen not to be
        instances of np.number.
        """
        if isinstance(value, bool) or isinstance(value, np.bool_):
            return None
        if isinstance(value, (int, float, np.floating, np.integer)):
            return float(value)
        if isinstance(value, np.ndarray) and value.ndim == 0:
            if np.issubdtype(value.dtype, np.floating) or np.issubdtype(
                value.dtype, np.integer
            ):
                return float(value)
        return None

    @staticmethod
    def _override_nodata(
        bands_meta: List[SampleDimension],
        nodata: Union[float, Sequence[float]],
        dtype: np.dtype,
    ) -> List[SampleDimension]:
        """Return bands_meta with NODATA replaced by the requested value(s)."""
        scalar = InDbSedonaRaster._as_nodata_scalar(nodata)
        if scalar is not None:
            values = [scalar] * len(bands_meta)
        else:
            try:
                entries = list(nodata)
            except TypeError:
                raise ValueError(
                    f"nodata must be a real number or a sequence of real numbers, "
                    f"got {type(nodata).__name__}"
                ) from None
            values = []
            for entry in entries:
                value = InDbSedonaRaster._as_nodata_scalar(entry)
                if value is None:
                    raise ValueError(
                        f"nodata entries must be real numbers, got "
                        f"{type(entry).__name__}"
                    )
                values.append(value)
            if len(values) != len(bands_meta):
                raise ValueError(
                    f"nodata has {len(values)} entries but the output has "
                    f"{len(bands_meta)} band(s)"
                )

        values = [
            InDbSedonaRaster._normalize_nodata_for_dtype(value, dtype, inherited=False)
            for value in values
        ]

        return [
            SampleDimension(
                description=bm.description,
                offset=bm.offset,
                scale=bm.scale,
                nodata=value,
            )
            for bm, value in zip(bands_meta, values)
        ]

    @staticmethod
    def _normalize_inherited_nodata(
        bands_meta: List[SampleDimension], dtype: np.dtype
    ) -> List[SampleDimension]:
        """Validate inherited NODATA against the output dtype.

        Without this, narrowing the dtype silently invalidates the metadata: a float64
        source whose NODATA is -9999 narrowed to uint8 output would keep declaring
        -9999, a value no uint8 pixel can hold, so every hole would read as valid data.
        """
        normalized_meta = []
        for bm in bands_meta:
            value = bm.nodata
            if np.isnan(value):
                normalized_meta.append(bm)
                continue
            normalized = InDbSedonaRaster._normalize_nodata_for_dtype(
                value, dtype, inherited=True
            )
            if normalized == value:
                normalized_meta.append(bm)
            else:
                normalized_meta.append(
                    SampleDimension(
                        description=bm.description,
                        offset=bm.offset,
                        scale=bm.scale,
                        nodata=normalized,
                    )
                )
        return normalized_meta

    # NODATA is stored using the JVM data buffer type, which does not always match the
    # NumPy dtype: int8 is stored as an unsigned byte and uint32 as a signed int (see
    # _numpy_dtype_to_data_buffer_type). Ranges are the JVM storage ranges.
    _NODATA_STORAGE_RANGES = {
        np.dtype(np.uint8): (0, 255),
        np.dtype(np.int8): (0, 255),
        np.dtype(np.int16): (-(2**15), 2**15 - 1),
        np.dtype(np.uint16): (0, 2**16 - 1),
        np.dtype(np.int32): (-(2**31), 2**31 - 1),
        np.dtype(np.uint32): (-(2**31), 2**31 - 1),
    }

    @staticmethod
    def _normalize_nodata_for_dtype(
        value: float, dtype: np.dtype, inherited: bool
    ) -> float:
        """Map a NODATA value onto what the JVM will actually store for this dtype.

        Integral bands reject fractional and out-of-range values, and reinterpret the
        two documented storage mismatches the same way the pixel data is reinterpreted:
        int8 negatives as unsigned bytes (-2 -> 254) and uint32 values above 2**31 - 1
        as their signed equivalent. float32 values are rounded to the nearest float32,
        since that is what the pixels themselves hold. Non-finite values other than NaN
        are rejected for every dtype.
        """
        if np.isnan(value):
            return value
        dtype = np.dtype(dtype)
        hint = (
            " (inherited from the source raster; pass nodata= explicitly, or clean the"
            " pixel data before changing its dtype)"
            if inherited
            else ""
        )
        if np.issubdtype(dtype, np.integer):
            if not np.isfinite(value) or value != int(value):
                raise ValueError(
                    f"nodata={value} is not representable in an integral output of "
                    f"dtype {dtype}{hint}; use a whole number, or cast the band data "
                    f"to a floating dtype first"
                )
            if dtype == np.dtype(np.int8) and -128 <= value < 0:
                # int8 pixels are stored as unsigned bytes, so -2 reads back as 254.
                # Reinterpret the nodata value the same way so it still marks the holes.
                value = value + 256
            elif dtype == np.dtype(np.uint32) and 2**31 <= value <= 2**32 - 1:
                # uint32 pixels are stored as signed ints, so 4294967295 reads back
                # as -1. Same reinterpretation.
                value = value - 2**32
            lo, hi = InDbSedonaRaster._NODATA_STORAGE_RANGES[dtype]
            if not (lo <= value <= hi):
                raise ValueError(
                    f"nodata={value} is outside the storage range of dtype {dtype} "
                    f"[{lo}, {hi}]{hint}"
                )
            return float(value)
        if dtype == np.dtype(np.float32):
            # Pixels hold float32 values, so the declared nodata has to be one too —
            # 0.1 would otherwise never equal the float32 0.1 the pixels carry.
            coerced = float(np.float32(value))
            if not np.isfinite(coerced):
                raise ValueError(
                    f"nodata={value} is not representable as float32{hint}"
                )
            return coerced
        if not np.isfinite(value):
            raise ValueError(f"nodata={value} must be finite or NaN{hint}")
        return float(value)

    def close(self):
        if self.rasterio_dataset_reader is not None:
            self.rasterio_dataset_reader.close()
            self.rasterio_dataset_reader = None
        if self.rasterio_memfile is not None:
            self.rasterio_memfile.close()
            self.rasterio_memfile = None
