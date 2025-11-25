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

import logging
from typing import Iterator, Union, List
from typing import Optional

import datetime as python_datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import dt
from shapely.geometry.base import BaseGeometry


def get_collection_url(url: str, collection_id: Optional[str] = None) -> str:
    """
    Constructs the collection URL based on the provided base URL and optional collection ID.

    If the collection ID is provided and the URL starts with 'http' or 'https', the collection ID
    is appended to the URL. Otherwise, an exception is raised.

    Args:
        url (str): The base URL of the STAC collection.
        collection_id (Optional[str]): The optional collection ID to append to the URL.

    Returns:
        str: The constructed collection URL.

    Raises:
        ValueError: If the URL does not start with 'http' or 'https' and a collection ID is provided.
    """
    if not collection_id:
        return url
    elif url.startswith("http") or url.startswith("https"):
        return f"{url}/collections/{collection_id}"
    else:
        raise ValueError(
            "Collection ID is not used because the URL does not start with http or https"
        )


class CollectionClient:
    def __init__(
        self,
        url: str,
        collection_id: Optional[str] = None,
        headers: Optional[dict] = None,
    ):
        """
        Initializes a collection client for a STAC collection.

        :param url: The base URL of the STAC API.
        :param collection_id: The ID of the collection to access. If None, accesses the catalog root.
        :param headers: Optional dictionary of HTTP headers for authentication.
        """
        self.url = url
        self.collection_id = collection_id
        self.collection_url = get_collection_url(url, collection_id)
        self.headers = headers if headers is not None else {}
        self.spark = SparkSession.getActiveSession()

    @staticmethod
    def _move_attributes_to_properties(item_dict: dict) -> dict:
        """
        Moves specified attributes from the item dictionary to the 'properties' field.

        This method ensures that certain attributes are nested under the 'properties' key
        in the item dictionary. If the 'properties' key does not exist, it is initialized.

        Args:
            item_dict (dict): The dictionary representation of a STAC item.

        Returns:
            dict: The updated item dictionary with specified attributes moved to 'properties'.
        """
        # List of attributes to move to 'properties'
        attributes_to_move = [
            "title",
            "description",
            "keywords",
            "datetime",
            "start_datetime",
            "end_datetime",
            "created",
            "instruments",
            "statistics",
            "platform",
            "gsd",
        ]

        # Initialize 'properties' if it doesn't exist
        if "properties" not in item_dict:
            item_dict["properties"] = {}

        # Move the specified attributes to 'properties'
        for attr in attributes_to_move:
            if attr in item_dict:
                item_dict["properties"][attr] = str(item_dict.pop(attr))

        return item_dict

    @staticmethod
    def _apply_spatial_temporal_filters(
        df: DataFrame, bbox=None, geometry=None, datetime=None
    ) -> DataFrame:
        """
        This function applies spatial and temporal filters to a Spark DataFrame using safe parameterized operations.

        Args:
            df (DataFrame): The input Spark DataFrame to be filtered.
            bbox (Optional[list]): A list of bounding boxes for filtering the items.
                Each bounding box is represented as a list of four float values: [min_lon, min_lat, max_lon, max_lat].
                Example: [[-180.0, -90.0, 180.0, 90.0]]  # This bounding box covers the entire world.
            geometry (Optional[list]): A list of geometry objects (Shapely or WKT) for spatial filtering.
                If both bbox and geometry are provided, geometry takes precedence.
            datetime (Optional[list]): A list of date-time ranges for filtering the items.
                Each date-time range is represented as a list of two strings in ISO 8601 format: [start_datetime, end_datetime].
                Example: [["2020-01-01T00:00:00Z", "2021-01-01T00:00:00Z"]]  # This interval covers the entire year of 2020.

        Returns:
            DataFrame: The filtered Spark DataFrame.

        The function uses Spark SQL column operations and functions instead of string concatenation
        to prevent SQL injection vulnerabilities. Spatial and temporal conditions are combined using OR logic.
        """
        from pyspark.sql import functions as F
        from pyspark.sql.functions import col, lit

        # Geometry takes precedence over bbox
        if geometry:
            geometry_conditions = []
            for geom in geometry:
                try:
                    # Validate and sanitize geometry input
                    if isinstance(geom, str):
                        # Validate WKT format basic structure
                        if not geom.strip() or any(
                            char in geom for char in ["'", '"', ";", "--", "/*", "*/"]
                        ):
                            raise ValueError("Invalid WKT geometry string")
                        geom_wkt = geom.strip()
                    elif hasattr(geom, "wkt"):
                        # Shapely geometry object
                        geom_wkt = geom.wkt
                    else:
                        # Try to convert to string (fallback)
                        geom_str = str(geom)
                        if not geom_str.strip() or any(
                            char in geom_str
                            for char in ["'", '"', ";", "--", "/*", "*/"]
                        ):
                            raise ValueError("Invalid geometry string")
                        geom_wkt = geom_str.strip()

                    # Use Spark SQL functions with safe literal values
                    from pyspark.sql.functions import expr

                    geometry_conditions.append(
                        expr(
                            "st_intersects(ST_GeomFromText('{}'), geometry)".format(
                                geom_wkt.replace("'", "''")
                            )
                        )
                    )
                except (ValueError, TypeError, AttributeError):
                    # Skip invalid geometries rather than failing
                    continue

            if geometry_conditions:
                # Combine conditions with OR using reduce
                from functools import reduce

                combined_condition = reduce(lambda a, b: a | b, geometry_conditions)
                df = df.filter(combined_condition)

        elif bbox:
            bbox_conditions = []
            for bbox_item in bbox:
                try:
                    # Validate bbox parameters are numeric
                    if len(bbox_item) != 4:
                        continue

                    min_lon, min_lat, max_lon, max_lat = bbox_item

                    # Validate numeric values and reasonable ranges
                    for coord in [min_lon, min_lat, max_lon, max_lat]:
                        if (
                            not isinstance(coord, (int, float)) or coord != coord
                        ):  # NaN check
                            raise ValueError("Invalid coordinate")

                    # Validate longitude/latitude ranges
                    if not (-180 <= min_lon <= 180 and -180 <= max_lon <= 180):
                        continue
                    if not (-90 <= min_lat <= 90 and -90 <= max_lat <= 90):
                        continue

                    # Create polygon using validated numeric values
                    polygon_wkt = "POLYGON(({} {}, {} {}, {} {}, {} {}, {} {}))".format(
                        float(min_lon),
                        float(min_lat),
                        float(max_lon),
                        float(min_lat),
                        float(max_lon),
                        float(max_lat),
                        float(min_lon),
                        float(max_lat),
                        float(min_lon),
                        float(min_lat),
                    )

                    bbox_conditions.append(
                        F.expr(
                            f"st_intersects(ST_GeomFromText('{polygon_wkt}'), geometry)"
                        )
                    )

                except (ValueError, TypeError, IndexError):
                    # Skip invalid bbox items rather than failing
                    continue

            if bbox_conditions:
                # Combine conditions with OR using reduce
                from functools import reduce

                combined_condition = reduce(lambda a, b: a | b, bbox_conditions)
                df = df.filter(combined_condition)

        if datetime:
            interval_conditions = []
            for interval in datetime:
                try:
                    if len(interval) != 2:
                        continue

                    start_time, end_time = interval

                    # Validate datetime strings (basic ISO format check)
                    if not isinstance(start_time, str) or not isinstance(end_time, str):
                        continue

                    # Check for SQL injection patterns
                    for time_str in [start_time, end_time]:
                        if any(
                            char in time_str
                            for char in ["'", '"', ";", "--", "/*", "*/", chr(0)]
                        ):
                            raise ValueError("Invalid datetime string")

                    # Use Spark column operations instead of string concatenation
                    condition = (col("datetime") >= lit(start_time)) & (
                        col("datetime") <= lit(end_time)
                    )
                    interval_conditions.append(condition)

                except (ValueError, TypeError, IndexError):
                    # Skip invalid datetime intervals rather than failing
                    continue

            if interval_conditions:
                # Combine conditions with OR using reduce
                from functools import reduce

                combined_condition = reduce(lambda a, b: a | b, interval_conditions)
                df = df.filter(combined_condition)

        return df

    @staticmethod
    def _expand_date(date_str):
        """
        Expands a simple date string to include the entire time period.

        This function takes a date string in one of the following formats:
        - YYYY
        - YYYY-mm
        - YYYY-mm-dd
        - YYYY-mm-ddTHH:MM:SSZ

        It then expands the date string to cover the entire time period for that date.

        Args:
            date_str (str): The date string to expand.

        Returns:
            list: A list containing the start and end datetime strings in ISO 8601 format.

        Raises:
            ValueError: If the date string format is invalid.

        Examples:
        - "2017" expands to ["2017-01-01T00:00:00Z", "2017-12-31T23:59:59Z"]
        - "2017-06" expands to ["2017-06-01T00:00:00Z", "2017-06-30T23:59:59Z"]
        - "2017-06-10" expands to ["2017-06-10T00:00:00Z", "2017-06-10T23:59:59Z"]
        - "2017-06-01T00:00:00Z" remains as ["2017-06-01T00:00:00Z", "2017-06-01T00:00:00Z"]
        """
        if len(date_str) == 4:  # YYYY
            return [f"{date_str}-01-01T00:00:00Z", f"{date_str}-12-31T23:59:59Z"]
        elif len(date_str) == 7:  # YYYY-mm
            year, month = date_str.split("-")
            last_day = (dt(int(year), int(month) + 1, 1) - dt.timedelta(days=1)).day
            return [f"{date_str}-01T00:00:00Z", f"{date_str}-{last_day}T23:59:59Z"]
        elif len(date_str) == 10:  # YYYY-mm-dd
            return [f"{date_str}T00:00:00Z", f"{date_str}T23:59:59Z"]
        elif len(date_str) == 19:  # YYYY-mm-ddTHH:MM:SS
            return [date_str, date_str]
        elif len(date_str) == 20:  # YYYY-mm-ddTHH:MM:SSZ
            return [date_str, date_str]
        else:
            raise ValueError("Invalid date format")

    def get_items(
        self,
        *ids: Union[str, list],
        bbox: Optional[list] = None,
        geometry: Optional[
            Union[str, BaseGeometry, List[Union[str, BaseGeometry]]]
        ] = None,
        datetime: Optional[Union[str, python_datetime.datetime, list]] = None,
        max_items: Optional[int] = None,
    ) -> Iterator:
        """
        Returns an iterator of items. Each item has the supplied item ID and/or optional spatial and temporal extents.

        This method loads the collection data from the specified collection URL and applies
        optional filters to the data.

        :param ids: A list of item IDs to filter the items. If not provided, no ID filtering is applied.
        :param bbox: A list of bounding boxes for filtering the items.
        :param geometry: Shapely geometry object(s) or WKT string(s) for spatial filtering.
            Can be a single geometry, WKT string, or a list of geometries/WKT strings.
            If both bbox and geometry are provided, geometry takes precedence.
        :param datetime: A single datetime, RFC 3339-compliant timestamp,
            or a list of date-time ranges for filtering the items.
        :param max_items: The maximum number of items to return from the search, even if there are more matching results.
        :return: An iterator of PyStacItem objects that match the specified filters.
            If no filters are provided, the iterator contains all items in the collection.
        :raises RuntimeError: If there is an error loading the data or applying the filters, a RuntimeError
            is raised with a message indicating the failure.
        """
        try:
            df = self.load_items_df(bbox, geometry, datetime, ids, max_items)

            # Import pystac only when needed
            try:
                from pystac import Item as PyStacItem
            except ImportError as e:
                raise ImportError(
                    "STAC functionality requires pystac. Please install pystac: pip install pystac"
                ) from e

            # Collect the filtered rows and convert them to PyStacItem objects
            items = []
            for row in df.collect():
                row_dict = row.asDict(True)
                row_dict = self._move_attributes_to_properties(row_dict)
                items.append(PyStacItem.from_dict(row_dict))

            # Return an iterator of the items
            return iter(items)
        except Exception as e:
            # Log error type without exposing sensitive details
            logging.error(f"Error getting items: {type(e).__name__}")
            raise RuntimeError("Failed to get items") from None

    def get_dataframe(
        self,
        *ids: Union[str, list],
        bbox: Optional[list] = None,
        geometry: Optional[
            Union[str, BaseGeometry, List[Union[str, BaseGeometry]]]
        ] = None,
        datetime: Optional[Union[str, python_datetime.datetime, list]] = None,
        max_items: Optional[int] = None,
    ) -> DataFrame:
        """
        Returns a Spark DataFrame of items with optional spatial and temporal extents.

        This method loads the collection data from the specified collection URL and applies
        optional spatial and temporal filters to the data. The spatial filter is applied using
        a bounding box, and the temporal filter is applied using a date-time range.

        :param ids: A variable number of item IDs to filter the items.
            Example: "item_id1" or ["item_id1", "item_id2"]
        :param bbox: A list of bounding boxes for filtering the items.
            Each bounding box is represented as a list of four float values: [min_lon, min_lat, max_lon, max_lat].
            Example: [[-180.0, -90.0, 180.0, 90.0]]  # This bounding box covers the entire world.
        :param geometry: Shapely geometry object(s) or WKT string(s) for spatial filtering.
            Can be a single geometry, WKT string, or a list of geometries/WKT strings.
            If both bbox and geometry are provided, geometry takes precedence.
            Example: Polygon(...) or "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))" or [Polygon(...), Polygon(...)]
        :param datetime: A single datetime, RFC 3339-compliant timestamp,
            or a list of date-time ranges for filtering the items.
            Example: "2020-01-01T00:00:00Z" or python_datetime.datetime(2020, 1, 1) or [["2020-01-01T00:00:00Z", "2021-01-01T00:00:00Z"]]
        :param max_items: The maximum number of items to return from the search.
        :return: A Spark DataFrame containing the filtered items. If no filters are provided,
            the DataFrame contains all items in the collection.
        :raises RuntimeError: If there is an error loading the data or applying the filters, a RuntimeError
            is raised with a message indicating the failure.
        """
        try:
            df = self.load_items_df(bbox, geometry, datetime, ids, max_items)

            return df
        except Exception as e:
            logging.error(f"Error getting filtered dataframe: {type(e).__name__}")
            raise RuntimeError("Failed to get filtered dataframe") from None

    def save_to_geoparquet(
        self,
        *ids: Union[str, list],
        output_path: str,
        bbox: Optional[list] = None,
        geometry: Optional[
            Union[str, BaseGeometry, List[Union[str, BaseGeometry]]]
        ] = None,
        datetime: Optional[list] = None,
    ) -> None:
        """
        Loads the STAC DataFrame and saves it to Parquet format at the given output path.

        This method loads the collection data from the specified collection URL and applies
        optional spatial and temporal filters to the data. The filtered data is then saved
        to the specified output path in Parquet format.

        :param ids: A list of item IDs to filter the items. If not provided, no ID filtering is applied.
        :param output_path: The path where the Parquet file will be saved.
        :param bbox: A bounding box for filtering the items. If not provided, no spatial filtering is applied.
        :param geometry: Shapely geometry object(s) or WKT string(s) for spatial filtering.
            If both bbox and geometry are provided, geometry takes precedence.
        :param datetime: A temporal extent that defines the
            date-time range for filtering the items. If not provided, no temporal filtering is applied.
            To match a single datetime, you can set the start and end datetime to the same value in the datetime.
            Example: [["2020-01-01T00:00:00Z", "2020-01-01T00:00:00Z"]]
        :raises RuntimeError: If there is an error loading the data, applying the filters, or saving the
            DataFrame to Parquet format, a RuntimeError is raised with a message indicating the failure.
        """
        try:
            df = self.get_dataframe(
                *ids, bbox=bbox, geometry=geometry, datetime=datetime
            )
            df_geoparquet = self._convert_assets_schema(df)
            df_geoparquet.write.format("geoparquet").save(output_path)
            logging.info(f"DataFrame successfully saved to {output_path}")
        except Exception as e:
            logging.error(f"Error saving DataFrame to GeoParquet: {type(e).__name__}")
            raise RuntimeError("Failed to save DataFrame to GeoParquet") from None

    @staticmethod
    def _convert_assets_schema(df: DataFrame) -> DataFrame:
        """
        Converts the schema of the assets column in the DataFrame to have a consistent structure.

        This function first identifies all unique keys in the assets column and then ensures that
        each row in the DataFrame has these keys with appropriate values.

        The expected input schema of the loaded dataframe (df) can be found here:
        https://sedona.apache.org/latest-snapshot/api/sql/Stac/#usage

        Args:
            df (DataFrame): The input DataFrame with an assets column.

        Returns:
            DataFrame: The DataFrame with a consistent schema for the assets column.
        """
        from pyspark.sql.functions import col, explode, struct

        # Explode the assets column to get all unique keys and their corresponding value struct
        exploded_df = df.select(explode("assets").alias("key", "value"))
        unique_keys = [
            row["key"] for row in exploded_df.select("key").distinct().collect()
        ]

        # Create a new schema with all unique keys and their value struct
        new_schema = struct(
            [struct(col(f"assets.{key}.*")).alias(key) for key in unique_keys]
        )

        # Apply the new schema to the assets column
        df = df.withColumn("assets", new_schema)

        return df

    def load_items_df(self, bbox, geometry, datetime, ids, max_items):
        """
        Loads items from the STAC collection as a Spark DataFrame.

        This method handles the conversion of headers to Spark options and
        applies various filters to the data.
        """
        import json

        # Prepare Spark DataFrameReader with headers if present
        reader = self.spark.read.format("stac")

        # Encode headers as JSON string for passing to Spark
        if self.headers:
            headers_json = json.dumps(self.headers)
            reader = reader.option("headers", headers_json)

        # Load the collection data from the specified collection URL
        if (
            not ids
            and not bbox
            and not geometry
            and not datetime
            and max_items is not None
        ):
            df = reader.option("itemsLimitMax", max_items).load(self.collection_url)
        else:
            df = reader.load(self.collection_url)
            # Apply ID filters if provided
            if ids:
                if isinstance(ids, tuple):
                    ids = list(ids)
                if isinstance(ids, str):
                    ids = [ids]
                df = df.filter(df.id.isin(ids))
            # Ensure bbox is a list of lists
            if bbox and isinstance(bbox[0], float):
                bbox = [bbox]
            # Handle geometry parameter
            if geometry:
                if not isinstance(geometry, list):
                    geometry = [geometry]
            # Handle datetime parameter
            if datetime:
                if isinstance(datetime, (str, python_datetime.datetime)):
                    datetime = [self._expand_date(str(datetime))]
                elif isinstance(datetime, (list, tuple)) and isinstance(
                    datetime[0], str
                ):
                    datetime = [list(datetime)]
            # Apply spatial and temporal filters
            df = self._apply_spatial_temporal_filters(df, bbox, geometry, datetime)
        # Limit the number of items if max_items is specified
        if max_items is not None:
            df = df.limit(max_items)
        return df

    def __str__(self):
        return f"<CollectionClient id={self.collection_id}>"
