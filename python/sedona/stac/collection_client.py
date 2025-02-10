#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import logging
from typing import Iterator, Union
from typing import Optional

import datetime as python_datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import dt
from pystac import Item as PyStacItem


def get_collection_url(url: str, collection_id: Optional[str] = None) -> str:
    """
    Constructs the collection URL based on the provided base URL and optional collection ID.

    If the collection ID is provided and the URL starts with 'http' or 'https', the collection ID
    is appended to the URL. Otherwise, an exception is raised.

    Parameters:
    - url (str): The base URL of the STAC collection.
    - collection_id (Optional[str]): The optional collection ID to append to the URL.

    Returns:
    - str: The constructed collection URL.

    Raises:
    - ValueError: If the URL does not start with 'http' or 'https' and a collection ID is provided.
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
    def __init__(self, url: str, collection_id: Optional[str] = None):
        self.url = url
        self.collection_id = collection_id
        self.collection_url = get_collection_url(url, collection_id)
        self.spark = SparkSession.getActiveSession()

    @staticmethod
    def _move_attributes_to_properties(item_dict: dict) -> dict:
        """
        Moves specified attributes from the item dictionary to the 'properties' field.

        This method ensures that certain attributes are nested under the 'properties' key
        in the item dictionary. If the 'properties' key does not exist, it is initialized.

        Parameters:
        - item_dict (dict): The dictionary representation of a STAC item.

        Returns:
        - dict: The updated item dictionary with specified attributes moved to 'properties'.
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
        df: DataFrame, bbox=None, datetime=None
    ) -> DataFrame:
        """
        This function applies spatial and temporal filters to a Spark DataFrame.

        Parameters:
        - df (DataFrame): The input Spark DataFrame to be filtered.
        - bbox (Optional[list]): A list of bounding boxes for filtering the items.
          Each bounding box is represented as a list of four float values: [min_lon, min_lat, max_lon, max_lat].
          Example: [[-180.0, -90.0, 180.0, 90.0]]  # This bounding box covers the entire world.
        - datetime (Optional[list]): A list of date-time ranges for filtering the items.
          Each date-time range is represented as a list of two strings in ISO 8601 format: [start_datetime, end_datetime].
          Example: [["2020-01-01T00:00:00Z", "2021-01-01T00:00:00Z"]]  # This interval covers the entire year of 2020.

        Returns:
        - DataFrame: The filtered Spark DataFrame.

        The function constructs SQL conditions for spatial and temporal filters and applies them to the DataFrame.
        If bbox is provided, it constructs spatial conditions using st_contains and ST_GeomFromText.
        If datetime is provided, it constructs temporal conditions using the datetime column.
        The conditions are combined using OR logic.
        """
        if bbox:
            bbox_conditions = []
            for bbox in bbox:
                polygon_wkt = (
                    f"POLYGON(({bbox[0]} {bbox[1]}, {bbox[2]} {bbox[1]}, "
                    f"{bbox[2]} {bbox[3]}, {bbox[0]} {bbox[3]}, {bbox[0]} {bbox[1]}))"
                )
                bbox_conditions.append(
                    f"st_contains(ST_GeomFromText('{polygon_wkt}'), geometry)"
                )
            bbox_sql_condition = " OR ".join(bbox_conditions)
            df = df.filter(bbox_sql_condition)

        if datetime:
            interval_conditions = []
            for interval in datetime:
                interval_conditions.append(
                    f"datetime BETWEEN '{interval[0]}' AND '{interval[1]}'"
                )
            interval_sql_condition = " OR ".join(interval_conditions)
            df = df.filter(interval_sql_condition)

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

        Parameters:
        - date_str (str): The date string to expand.

        Returns:
        - list: A list containing the start and end datetime strings in ISO 8601 format.

        Raises:
        - ValueError: If the date string format is invalid.

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
        datetime: Optional[Union[str, python_datetime.datetime, list]] = None,
        max_items: Optional[int] = None,
    ) -> Iterator[PyStacItem]:
        """
        Returns an iterator of items. Each item has the supplied item ID and/or optional spatial and temporal extents.

        This method loads the collection data from the specified collection URL and applies
        optional filters to the data. The filters include:
        - IDs: A list of item IDs to filter the items. If not provided, no ID filtering is applied.
        - bbox (Optional[list]): A list of bounding boxes for filtering the items.
        - datetime (Optional[Union[str, python_datetime.datetime, list]]): A single datetime, RFC 3339-compliant timestamp,
          or a list of date-time ranges for filtering the items.
        - max_items (Optional[int]): The maximum number of items to return from the search, even if there are more matching results.

        Returns:
        - Iterator[PyStacItem]: An iterator of PyStacItem objects that match the specified filters.
          If no filters are provided, the iterator contains all items in the collection.

        Raises:
        - RuntimeError: If there is an error loading the data or applying the filters, a RuntimeError
          is raised with a message indicating the failure.
        """
        try:
            # Load the collection data from the specified collection URL
            df = self.spark.read.format("stac").load(self.collection_url)

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

            # Handle datetime parameter
            if datetime:
                if isinstance(datetime, (str, python_datetime.datetime)):
                    datetime = [self._expand_date(str(datetime))]
                elif isinstance(datetime, list) and isinstance(datetime[0], str):
                    datetime = [datetime]

            # Apply spatial and temporal filters
            df = self._apply_spatial_temporal_filters(df, bbox, datetime)

            # Limit the number of items if max_items is specified
            if max_items is not None:
                df = df.limit(max_items)

            # Collect the filtered rows and convert them to PyStacItem objects
            items = []
            for row in df.collect():
                row_dict = row.asDict(True)
                row_dict = self._move_attributes_to_properties(row_dict)
                items.append(PyStacItem.from_dict(row_dict))

            # Return an iterator of the items
            return iter(items)
        except Exception as e:
            # Log the error and raise a RuntimeError
            logging.error(f"Error getting items: {e}")
            raise RuntimeError("Failed to get items") from e

    def get_dataframe(
        self,
        *ids: Union[str, list],
        bbox: Optional[list] = None,
        datetime: Optional[Union[str, python_datetime.datetime, list]] = None,
        max_items: Optional[int] = None,
    ) -> DataFrame:
        """
        Returns a Spark DataFrame of items with optional spatial and temporal extents.

        This method loads the collection data from the specified collection URL and applies
        optional spatial and temporal filters to the data. The spatial filter is applied using
        a bounding box, and the temporal filter is applied using a date-time range.

        Parameters:
        - bbox (Optional[list]): A list of bounding boxes for filtering the items.
          Each bounding box is represented as a list of four float values: [min_lon, min_lat, max_lon, max_lat].
          Example: [[-180.0, -90.0, 180.0, 90.0]]  # This bounding box covers the entire world.
        - datetime (Optional[Union[str, python_datetime.datetime, list]]): A single datetime, RFC 3339-compliant timestamp,
          or a list of date-time ranges for filtering the items.
          Example: "2020-01-01T00:00:00Z" or python_datetime.datetime(2020, 1, 1) or [["2020-01-01T00:00:00Z", "2021-01-01T00:00:00Z"]]

        Returns:
        - DataFrame: A Spark DataFrame containing the filtered items. If no filters are provided,
          the DataFrame contains all items in the collection.

        Raises:
        - RuntimeError: If there is an error loading the data or applying the filters, a RuntimeError
          is raised with a message indicating the failure.
        """
        try:
            df = self.spark.read.format("stac").load(self.collection_url)

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

            # Handle datetime parameter
            if datetime:
                if isinstance(datetime, (str, python_datetime.datetime)):
                    datetime = [[str(datetime), str(datetime)]]
                elif isinstance(datetime, list) and isinstance(datetime[0], str):
                    datetime = [datetime]

            df = self._apply_spatial_temporal_filters(df, bbox, datetime)

            # Limit the number of items if max_items is specified
            if max_items is not None:
                df = df.limit(max_items)

            return df
        except Exception as e:
            logging.error(f"Error getting filtered dataframe: {e}")
            raise RuntimeError("Failed to get filtered dataframe") from e

    def save_to_geoparquet(
        self,
        *ids: Union[str, list],
        output_path: str,
        bbox: Optional[list] = None,
        datetime: Optional[list] = None,
    ) -> None:
        """
        Loads the STAC DataFrame and saves it to Parquet format at the given output path.

        This method loads the collection data from the specified collection URL and applies
        optional spatial and temporal filters to the data. The filtered data is then saved
        to the specified output path in Parquet format.

        Parameters:
        - output_path (str): The path where the Parquet file will be saved.
        - spatial_extent (Optional[SpatialExtent]): A spatial extent object that defines the
          bounding box for filtering the items. If not provided, no spatial filtering is applied.
        - temporal_extent (Optional[TemporalExtent]): A temporal extent object that defines the
          date-time range for filtering the items. If not provided, no temporal filtering is applied.
          To match a single datetime, you can set the start and end datetime to the same value in the datetime.
          Here is an example:  [["2020-01-01T00:00:00Z", "2020-01-01T00:00:00Z"]]

        Raises:
        - RuntimeError: If there is an error loading the data, applying the filters, or saving the
          DataFrame to Parquet format, a RuntimeError is raised with a message indicating the failure.
        """
        try:
            df = self.get_dataframe(*ids, bbox=bbox, datetime=datetime)
            df_geoparquet = self._convert_assets_schema(df)
            df_geoparquet.write.format("geoparquet").save(output_path)
            logging.info(f"DataFrame successfully saved to {output_path}")
        except Exception as e:
            logging.error(f"Error saving DataFrame to GeoParquet: {e}")
            raise RuntimeError("Failed to save DataFrame to GeoParquet") from e

    @staticmethod
    def _convert_assets_schema(df: DataFrame) -> DataFrame:
        """
        Converts the schema of the assets column in the DataFrame to have a consistent structure.

        This function first identifies all unique keys in the assets column and then ensures that
        each row in the DataFrame has these keys with appropriate values.

        The expected input schema of the loaded dataframe (df) can be found here:
        https://sedona.apache.org/latest-snapshot/api/sql/Stac/#usage

        Parameters:
        - df (DataFrame): The input DataFrame with an assets column.

        Returns:
        - DataFrame: The DataFrame with a consistent schema for the assets column.
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

    def __str__(self):
        return f"<CollectionClient id={self.collection_id}>"
