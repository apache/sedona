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

"""Conservative distributed coverage simplification using spatial joins.

Coordinates and current/proposed segments stay in Spark. Reconstruction collects
only each admitted ring/polygon, never a whole coverage or neighborhood. Inputs
must form a valid edge-matched coverage; that cross-row precondition is not
validated here. Ring/part deletion and JTS-equivalent output are not promised.
"""

import logging
import math

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType
from pyspark.sql.utils import is_remote

from sedona.spark.sql.types import GeometryType


class _Checkpoints:
    """Own only the reliable checkpoint files made by one invocation.

    Spark allocates a unique rdd-N path for each raw checkpoint DataFrame. Never
    list or delete the configured root: other operations can use it concurrently.
    The returned output is detached from this owner and follows Spark/user
    checkpoint retention. As with any reliable Spark checkpoint, its files must
    remain available for as long as callers use the output.
    """

    def __init__(self, context):
        self._context = context
        self._paths = {}

    def save(self, frame):
        saved = frame.checkpoint(eager=True)
        # Use the raw LogicalRDD, not DataFrame.rdd (which adds a Python RDD).
        path = saved._jdf.queryExecution().analyzed().rdd().getCheckpointFile().get()
        self._paths[id(saved)] = path
        return saved

    def keep(self, frame):
        self._paths.pop(id(frame))
        return frame

    def release(self, frame):
        path = self._paths.pop(id(frame))
        self._delete(path)

    def _delete(self, path):
        # Paths originate only from successfully completed checkpoints above.
        # Do not mask a validation or Spark failure if storage cleanup fails.
        try:
            jpath = self._context._jvm.org.apache.hadoop.fs.Path(path)
            fs = jpath.getFileSystem(self._context._jsc.hadoopConfiguration())
            if not fs.delete(jpath, True) and fs.exists(jpath):
                logging.getLogger(__name__).warning(
                    "Could not remove intermediate coverage checkpoint %s", path
                )
        except Exception:
            logging.getLogger(__name__).warning(
                "Could not remove intermediate coverage checkpoint %s",
                path,
                exc_info=True,
            )

    def close(self):
        for path in self._paths.values():
            self._delete(path)
        self._paths.clear()


def _extract(frame: DataFrame) -> DataFrame:
    """Expand rings without closing duplicates; normalize signed-zero XY IDs."""
    parts = frame.select(
        "id",
        F.posexplode(F.expr("ST_Dump(geom)")).alias("part", "_polygon"),
    ).where("NOT ST_IsEmpty(_polygon)")
    rings = (
        parts.select(
            "id",
            "part",
            "_polygon",
            F.explode(
                F.sequence(F.lit(0), F.expr("ST_NumInteriorRings(_polygon)"))
            ).alias("ring"),
        )
        .withColumn(
            "_line",
            F.expr(
                "CASE WHEN ring = 0 THEN ST_ExteriorRing(_polygon) "
                "ELSE ST_InteriorRingN(_polygon, ring - 1) END"
            ),
        )
        .withColumn("_points", F.expr("ST_DumpPoints(_line)"))
    )
    # Filtering closing coordinates after expansion can retain source arrays
    # in Spark's plan and be expensive even within the input admission limit.
    points = rings.select(
        "id",
        "part",
        "ring",
        (F.size("_points") - 1).alias("_n"),
        F.posexplode("_points").alias("pos", "_point"),
    ).where(F.col("pos") < F.col("_n"))
    points = points.withColumn("_x", F.expr("ST_X(_point)")).withColumn(
        "_y", F.expr("ST_Y(_point)")
    )
    points = points.select(
        "id",
        "part",
        "ring",
        "pos",
        F.when(F.col("_x") == 0.0, F.lit(0.0)).otherwise(F.col("_x")).alias("x"),
        F.when(F.col("_y") == 0.0, F.lit(0.0)).otherwise(F.col("_y")).alias("y"),
    )
    return points.select(
        "id",
        "part",
        "ring",
        "pos",
        F.to_json(F.array("x", "y")).alias("vid"),
        "x",
        "y",
    )


def _segments(occ: DataFrame) -> DataFrame:
    """Canonical undirected segments with distinct owning-ring counts."""
    order = Window.partitionBy("id", "part", "ring").orderBy("pos")
    first = order.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    edges = occ
    for value in ("vid", "x", "y"):
        edges = edges.withColumn(
            "_next_" + value,
            F.coalesce(F.lead(value).over(order), F.first(value).over(first)),
        )
    forward = F.col("vid") < F.col("_next_vid")
    edges = edges.where(F.col("vid") != F.col("_next_vid")).select(
        "id",
        "part",
        "ring",
        F.when(forward, F.col("vid")).otherwise(F.col("_next_vid")).alias("a"),
        F.when(forward, F.col("_next_vid")).otherwise(F.col("vid")).alias("b"),
        F.when(forward, F.col("x")).otherwise(F.col("_next_x")).alias("ax"),
        F.when(forward, F.col("y")).otherwise(F.col("_next_y")).alias("ay"),
        F.when(forward, F.col("_next_x")).otherwise(F.col("x")).alias("bx"),
        F.when(forward, F.col("_next_y")).otherwise(F.col("y")).alias("by"),
    )
    edges = edges.groupBy("a", "b").agg(
        *[F.first(name).alias(name) for name in ("ax", "ay", "bx", "by")],
        F.countDistinct(F.struct("id", "part", "ring")).alias("owners"),
    )
    return edges.select(
        "a",
        "b",
        "ax",
        "ay",
        "bx",
        "by",
        F.expr("ST_MakeLine(array(ST_Point(ax, ay), ST_Point(bx, by)))").alias("geom"),
        "owners",
    )


def _candidates(occ, edges, tolerance, simplify_boundary):
    adjacency = edges.selectExpr(
        "a AS vid", "b AS nid", "bx AS nx", "by AS ny", "owners"
    ).unionByName(
        edges.selectExpr("b AS vid", "a AS nid", "ax AS nx", "ay AS ny", "owners")
    )
    pair = (
        adjacency.groupBy("vid")
        .agg(
            F.count("*").alias("degree"),
            F.min(F.struct("nid", "nx", "ny")).alias("u"),
            F.max(F.struct("nid", "nx", "ny")).alias("w"),
            F.min("owners").alias("min_owners"),
        )
        .where("degree = 2 AND u.nid <> w.nid")
    )
    if not simplify_boundary:
        pair = pair.where("min_owners = 2")
    sizes = (
        occ.groupBy("id", "part", "ring")
        .count()
        .withColumnRenamed("count", "ring_size")
    )
    minimum = (
        occ.join(sizes, ["id", "part", "ring"])
        .groupBy("vid")
        .agg(F.min("ring_size").alias("min_ring_size"))
    )
    points = occ.select("vid", "x", "y").distinct()
    return (
        pair.join(points, "vid")
        .join(minimum, "vid")
        .where("min_ring_size >= 4")
        .selectExpr(
            "vid",
            "x",
            "y",
            "u.nid uid",
            "w.nid wid",
            "u.nx ux",
            "u.ny uy",
            "w.nx wx",
            "w.ny wy",
        )
        .withColumn(
            "_triangle",
            F.expr(
                "ST_MakePolygon(ST_MakeLine(array("
                "ST_Point(ux,uy),ST_Point(x,y),ST_Point(wx,wy),ST_Point(ux,uy))))"
            ),
        )
        .withColumn("area", F.expr("ST_Area(_triangle)"))
        .where(F.col("area") <= tolerance * tolerance)
        .withColumn("priority", F.struct("area", "vid"))
        .withColumn(
            "ends", F.expr("ST_Collect(array(ST_Point(ux,uy), ST_Point(wx,wy)))")
        )
        # Robust orientation, not a rounded area, determines the dimension.
        # Keep the original triangle's area: hull ordering can change floating-
        # point cancellation for very thin triangles.
        .withColumn("footprint", F.expr("ST_ConvexHull(_triangle)"))
        # Skip non-collinear edits whose area rounds to zero.
        .where("area > 0 OR ST_GeometryType(footprint) = 'ST_LineString'")
        .drop("_triangle")
    )


def _rebuild(occ: DataFrame, original: DataFrame) -> DataFrame:
    """Rebuild original types, rings, parts, SRIDs, and all extra columns."""
    rings = (
        occ.groupBy("id", "part", "ring")
        .agg(F.sort_array(F.collect_list(F.struct("pos", "x", "y"))).alias("_coords"))
        .withColumn("_points", F.expr("transform(_coords, p -> ST_Point(p.x, p.y))"))
        .withColumn(
            "_line",
            F.expr("ST_MakeLine(concat(_points, array(element_at(_points, 1))))"),
        )
    )
    # Compare integer IDs, not GeometryUDT values.
    ring_arrays = rings.groupBy("id", "part").agg(
        F.array_sort(
            F.collect_list(F.struct("ring", "_line")),
            lambda left, right: left["ring"] - right["ring"],
        ).alias("_rings")
    )
    parts = ring_arrays.select(
        "id",
        "part",
        F.expr(
            "ST_MakePolygon("
            "element_at(filter(_rings, r -> r.ring = 0), 1)._line, "
            "transform(filter(_rings, r -> r.ring > 0), r -> r._line))"
        ).alias("_polygon"),
    )
    # Empty members have no coordinate occurrences. Restore them at their
    # original positions rather than dropping them while assembling the parts.
    original_parts = original.select(
        "id",
        F.expr("ST_SRID(geom)").alias("_srid"),
        F.posexplode(F.expr("ST_Dump(geom)")).alias("part", "_original_polygon"),
    )
    parts = original_parts.join(parts, ["id", "part"], "left").select(
        "id",
        "part",
        # ST_SetSRID copies geometry through GeometryEditor, which removes
        # empty collection members. Set each polygon's SRID before collecting.
        F.expr(
            "ST_SetSRID(CASE WHEN ST_IsEmpty(_original_polygon) "
            "THEN _original_polygon ELSE _polygon END, _srid)"
        ).alias("_polygon"),
    )
    polygons = parts.groupBy("id").agg(
        F.array_sort(
            F.collect_list(F.struct("part", "_polygon")),
            lambda left, right: left["part"] - right["part"],
        ).alias("_parts")
    )
    assembled = original.select("id", F.col("geom").alias("_original")).join(
        polygons, "id", "left"
    )
    result = F.expr(
        "CASE WHEN _original IS NULL OR ST_IsEmpty(_original) THEN _original "
        "ELSE CASE WHEN ST_GeometryType(_original) = 'ST_Polygon' "
        "THEN element_at(_parts, 1)._polygon "
        "ELSE ST_Collect(transform(_parts, p -> p._polygon)) END END"
    )
    rebuilt = assembled.select("id", result.alias("geom"))
    joined = original.alias("original").join(rebuilt.alias("rebuilt"), "id", "left")
    return joined.select(
        *[
            F.col(
                f"{'rebuilt' if field.name == 'geom' else 'original'}.`{field.name.replace('`', '``')}`"
            ).alias(field.name, metadata=field.metadata)
            for field in original.schema
        ]
    )


def simplify_coverage(
    frame: DataFrame, tolerance: float, simplify_boundary: bool
) -> DataFrame:
    """Simplify until the conservative selection has no further removals.

    ``frame`` has unique nonnull LongType ``id``, GeometryType ``geom``, and
    arbitrary additional columns. A configured shared Spark checkpoint directory
    is required. Execution is eager; the returned reliable checkpoint remains
    reusable without an explicit close operation. Its storage is retained under
    Spark/user lifecycle, not deleted by subsequent calls to this function.

    Each input geometry is limited to 100000 coordinates. The baseline can
    still be costly on long rings, dense spatial matches, or many rounds.
    Empty interior rings are rejected because the polygon constructor removes
    them. Empty polygon rows and empty multipart members are preserved.
    """
    if is_remote():
        raise NotImplementedError("coverage simplification requires Spark Classic")
    if (
        not isinstance(tolerance, (float, int))
        or not math.isfinite(tolerance)
        or tolerance < 0
    ):
        raise ValueError("tolerance must be a finite nonnegative scalar")
    if not isinstance(frame.schema["id"].dataType, LongType):
        raise ValueError("coverage simplification requires LongType row IDs")
    if not isinstance(frame.schema["geom"].dataType, GeometryType):
        raise ValueError("coverage simplification requires a GeometryType geom column")
    context = frame.sparkSession.sparkContext
    if not context.getCheckpointDir():
        raise ValueError(
            "coverage simplification requires a shared Spark checkpoint directory; "
            "configure spark.sparkContext.setCheckpointDir(...)"
        )
    checkpoints = _Checkpoints(context)
    try:
        # Snapshot before validation or any branches: physical IDs (and user
        # columns) may originate from nondeterministic Spark expressions.
        original = checkpoints.save(frame)
        stats = (
            original.selectExpr(
                "id",
                "ST_NPoints(geom) n",
                "CASE WHEN geom IS NULL THEN false ELSE "
                "ST_GeometryType(geom) NOT IN ('ST_Polygon','ST_MultiPolygon') OR "
                "NOT ST_IsValid(geom) OR ST_HasZ(geom) OR ST_HasM(geom) END invalid",
            )
            .agg(
                F.count("*").alias("rows"),
                F.countDistinct("id").alias("ids"),
                F.sum("n").alias("vertices"),
                F.max("n").alias("max_vertices"),
                F.max(F.col("invalid").cast("int")).alias("invalid"),
            )
            .first()
        )
        if stats.rows != stats.ids:
            raise ValueError("coverage simplification requires unique nonnull IDs")
        if stats.invalid:
            raise ValueError(
                "coverage simplification requires valid 2D Polygon/MultiPolygon rows"
            )
        if (stats.max_vertices or 0) > 100000:
            raise ValueError(
                "input exceeds the per-geometry limit of 100000 coordinates"
            )
        # Geometry-level dimension predicates inspect only the first
        # coordinate. A NaN first ordinate can hide later Z/M coordinates.
        # Inspect individual points only after the per-geometry size check.
        if (
            original.where("exists(ST_DumpPoints(geom), p -> ST_HasZ(p) OR ST_HasM(p))")
            .limit(1)
            .count()
        ):
            raise ValueError(
                "coverage simplification requires valid 2D Polygon/MultiPolygon rows"
            )
        if (
            original.where(
                "exists(ST_Dump(geom), p -> CASE WHEN ST_NumInteriorRings(p) = 0 "
                "THEN false ELSE exists(sequence(0, ST_NumInteriorRings(p) - 1), "
                "i -> ST_IsEmpty(ST_InteriorRingN(p, i))) END)"
            )
            .limit(1)
            .count()
        ):
            raise ValueError(
                "coverage simplification does not support empty interior rings"
            )
        if not stats.vertices:
            return checkpoints.keep(original)

        occ = checkpoints.save(_extract(original))
        bad = occ.where(
            "isnan(x) OR isnan(y) OR abs(x) = cast('Infinity' AS double) "
            "OR abs(y) = cast('Infinity' AS double)"
        )
        if bad.limit(1).count():
            raise ValueError("coverage simplification requires finite XY coordinates")
        if (
            occ.groupBy("id", "part", "ring", "vid")
            .count()
            .where("count > 1")
            .limit(1)
            .count()
        ):
            raise ValueError(
                "coverage simplification does not support repeated vertices within a ring"
            )

        while True:
            resources = []

            def materialize(value):
                saved = checkpoints.save(value)
                resources.append(saved)
                return saved

            try:
                edge = materialize(_segments(occ))
                candidates = materialize(
                    _candidates(occ, edge, tolerance, simplify_boundary)
                )
                if not candidates.limit(1).count():
                    break
                # Aggregate scalar flags, never arrays of neighboring geometry.
                hits = (
                    candidates.alias("c")
                    .join(
                        edge.alias("s"),
                        F.expr(
                            "ST_Intersects(c.footprint,s.geom) AND s.a <> c.vid AND s.b <> c.vid"
                        ),
                    )
                    .selectExpr(
                        "c.vid", "c.ends", "ST_Intersection(c.footprint,s.geom) hit"
                    )
                )
                flags = hits.groupBy("vid").agg(
                    F.max((~F.expr("ST_CoveredBy(hit,ends)")).cast("int")).alias(
                        "blocked"
                    )
                )
                safe = materialize(
                    candidates.join(flags, "vid", "left").where(
                        "coalesce(blocked,0) = 0"
                    )
                )
                # At most one coordinate per ring per round; all owners agree.
                memberships = occ.select("id", "part", "ring", "vid").join(
                    safe.select("vid", "priority"), "vid"
                )
                ring_min = memberships.groupBy("id", "part", "ring").agg(
                    F.min("priority").alias("winner")
                )
                ring_losers = (
                    memberships.join(ring_min, ["id", "part", "ring"])
                    .where("priority <> winner")
                    .select("vid")
                    .distinct()
                )
                winners = materialize(safe.join(ring_losers, "vid", "left_anti"))
                # Proposed affected regions must be pairwise disjoint.
                conflicts = (
                    winners.alias("a")
                    .join(
                        winners.alias("b"),
                        F.expr(
                            "ST_Intersects(a.footprint,b.footprint) AND a.priority > b.priority"
                        ),
                    )
                    .selectExpr("a.vid")
                    .distinct()
                )
                selected = materialize(
                    winners.join(conflicts, "vid", "left_anti").select("vid")
                )
                if not selected.limit(1).count():
                    break
                next_occ = checkpoints.save(occ.join(selected, "vid", "left_anti"))
                checkpoints.release(occ)
                occ = next_occ
            finally:
                for saved in resources:
                    checkpoints.release(saved)
        # Reliable output must complete before original/occ dependencies go away.
        output = checkpoints.save(_rebuild(occ, original))
        return checkpoints.keep(output)
    finally:
        checkpoints.close()
