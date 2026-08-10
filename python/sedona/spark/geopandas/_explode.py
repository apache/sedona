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

from __future__ import annotations

import typing

from pyspark.pandas.internal import (
    InternalFrame,
    NATURAL_ORDER_COLUMN_NAME,
    SPARK_DEFAULT_INDEX_NAME,
    SPARK_INDEX_NAME_FORMAT,
)
from pyspark.pandas.utils import scol_for, verify_temp_column_name
from pyspark.sql import functions as F


def expand_geometry_column(
    source_internal: InternalFrame,
    geometry_position: int,
    array_builder,
    ignore_index: bool,
    index_parts: bool,
    temp_prefix: str,
) -> InternalFrame:
    """Expand one geometry column and carry the complete frame alongside it."""

    internal = source_internal.resolved_copy
    source_sdf = internal.spark_frame
    reserved_names = set(source_sdf.columns)

    def temp_column_name(base: str) -> str:
        suffix = 0
        candidate = f"__{temp_prefix}_{base}__"
        while candidate in reserved_names:
            suffix += 1
            candidate = f"__{temp_prefix}_{base}_{suffix}__"
        reserved_names.add(candidate)
        return typing.cast(str, verify_temp_column_name(source_sdf, candidate))

    # A fresh distributed index is built below when ``ignore_index`` is set,
    # so the source index does not need to participate in the ordering shuffle.
    index_column_names = (
        []
        if ignore_index
        else [
            temp_column_name(f"index_{level}")
            for level in range(len(internal.index_spark_columns))
        ]
    )
    data_column_names = [
        temp_column_name(f"data_{position}")
        for position in range(len(internal.data_spark_columns))
    ]
    parent_order_col = temp_column_name("parent_order")
    position_col = temp_column_name("position")
    value_col = temp_column_name("value")
    sequence_col = temp_column_name("sequence")

    retained_data_columns = [
        column.alias(name, metadata=field.metadata)
        for position, (column, name, field) in enumerate(
            zip(
                internal.data_spark_columns,
                data_column_names,
                internal.data_fields,
            )
        )
        if position != geometry_position
    ]
    expanded_sdf = source_sdf.select(
        *[
            column.alias(name, metadata=field.metadata)
            for column, name, field in zip(
                internal.index_spark_columns,
                index_column_names,
                internal.index_fields,
            )
        ],
        *retained_data_columns,
        scol_for(source_sdf, NATURAL_ORDER_COLUMN_NAME).alias(parent_order_col),
        F.posexplode(
            array_builder(internal.data_spark_columns[geometry_position])
        ).alias(position_col, value_col),
    ).orderBy(parent_order_col, position_col)

    # The distributed sequence supplies both ignore_index and a stable
    # natural-order column without a single-partition row-number window.
    expanded_sdf = InternalFrame.attach_distributed_sequence_column(
        expanded_sdf, sequence_col
    )

    if ignore_index:
        output_index_names = [SPARK_DEFAULT_INDEX_NAME]
        index_names = [None]
        index_fields = None
        index_expressions = [
            scol_for(expanded_sdf, sequence_col).alias(SPARK_DEFAULT_INDEX_NAME)
        ]
    else:
        output_index_names = list(index_column_names)
        index_names = list(internal.index_names)
        index_fields = [
            field.copy(name=name)
            for field, name in zip(internal.index_fields, output_index_names)
        ]
        index_expressions = [
            scol_for(expanded_sdf, name) for name in output_index_names
        ]

        if index_parts:
            part_index_col = SPARK_INDEX_NAME_FORMAT(len(output_index_names))
            output_index_names.append(part_index_col)
            index_names.append(None)
            index_fields.append(None)
            index_expressions.append(
                scol_for(expanded_sdf, position_col).cast("long").alias(part_index_col)
            )

    data_expressions = []
    for position, (name, field) in enumerate(
        zip(data_column_names, internal.data_fields)
    ):
        source_name = value_col if position == geometry_position else name
        data_expressions.append(
            scol_for(expanded_sdf, source_name).alias(
                name,
                metadata=field.metadata,
            )
        )

    output_sdf = expanded_sdf.select(
        *index_expressions,
        *data_expressions,
        scol_for(expanded_sdf, sequence_col).alias(NATURAL_ORDER_COLUMN_NAME),
    )
    output_schema = output_sdf.schema
    data_fields = [
        field.copy(
            name=name,
            spark_type=output_schema[name].dataType,
            nullable=output_schema[name].nullable,
        )
        for name, field in zip(data_column_names, internal.data_fields)
    ]

    return internal.copy(
        spark_frame=output_sdf,
        index_spark_columns=[scol_for(output_sdf, name) for name in output_index_names],
        index_names=index_names,
        index_fields=index_fields,
        data_spark_columns=[scol_for(output_sdf, name) for name in data_column_names],
        data_fields=data_fields,
    )
