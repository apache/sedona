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

import unittest
import sys
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession, DataFrame
from sedona.spark import *


class TestToSedonaDB(unittest.TestCase):

    def setUp(self):
        # Mock sedona.db to avoid ImportError
        self.mock_sedona_db = MagicMock()
        sys.modules["sedona.db"] = self.mock_sedona_db
        import sedona

        sedona.db = self.mock_sedona_db
        self.spark = SparkSession.builder.getOrCreate()

    def tearDown(self):
        if "sedona.db" in sys.modules:
            del sys.modules["sedona.db"]
        import sedona

        if hasattr(sedona, "db"):
            del sedona.db

    @patch("sedona.spark.dataframe_to_arrow")
    def test_to_sedonadb_no_connection(self, mock_dataframe_to_arrow):
        # Mock dependencies
        mock_arrow_table = MagicMock()
        mock_dataframe_to_arrow.return_value = mock_arrow_table

        mock_connection = MagicMock()
        self.mock_sedona_db.connect.return_value = mock_connection

        mock_sedonadb_df = MagicMock()
        mock_connection.create_data_frame.return_value = mock_sedonadb_df

        # Create a dummy Spark DataFrame
        df = self.spark.range(1)

        # Call the method
        result = df.to_sedonadb()

        # Verify calls
        self.mock_sedona_db.connect.assert_called_once()
        mock_dataframe_to_arrow.assert_called_once_with(df)
        mock_connection.create_data_frame.assert_called_once_with(mock_arrow_table)
        self.assertEqual(result, mock_sedonadb_df)

    @patch("sedona.spark.dataframe_to_arrow")
    def test_to_sedonadb_with_connection(self, mock_dataframe_to_arrow):
        # Mock dependencies
        mock_arrow_table = MagicMock()
        mock_dataframe_to_arrow.return_value = mock_arrow_table

        mock_connection = MagicMock()
        mock_sedonadb_df = MagicMock()
        mock_connection.create_data_frame.return_value = mock_sedonadb_df

        # Create a dummy Spark DataFrame
        df = self.spark.range(1)

        # Call the method
        result = df.to_sedonadb(connection=mock_connection)

        # Verify calls
        mock_dataframe_to_arrow.assert_called_once_with(df)
        mock_connection.create_data_frame.assert_called_once_with(mock_arrow_table)
        self.assertEqual(result, mock_sedonadb_df)


if __name__ == "__main__":
    unittest.main()
