"""Two-ended flow reconciliation over the real Edge topology.

A test-bench connection (an energydb *edge*) records flow at both ends.
This module compares the two endpoint series for a shared window:

* series are selected by their **real topology identity** — edges are
  discovered from the database and each endpoint is resolved to its
  materialized node path, never by a hard-coded name;
* the two sides are aligned on ``valid_time`` (the effective timestamp);
* a diff row is reported **only where both sides have a reading** for the
  same ``valid_time`` (an inner, both-present join);
* every reading without a counterpart on the other side goes to the
  **coverage table** instead of being silently dropped or faked.

The public entry point is :func:`reconcile_edges`, which returns a
:class:`ReconciliationReport`. Everything is read-only.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import NamedTuple

import energydb as edb
import polars as pl


class EndpointIdentity(NamedTuple):
    """Where one side of a reconciliation actually lives in the topology."""

    edge_uuid: str
    edge_type: str
    edge_name: str | None
    node_path: str
    role: str  # "from" | "to"
    data_type: str
    series_name: str


@dataclass(frozen=True)
class ReconciliationReport:
    """The two output tables plus the identities they were built from.

    ``differences`` holds only timestamps present on **both** ends::

        edge_uuid, edge_type, edge_name, valid_time,
        from_path, from_value, to_path, to_value, diff (from - to), abs_diff

    ``coverage`` holds readings present on only one end::

        edge_uuid, edge_type, edge_name, valid_time, side ("from"|"to"),
        node_path, value

    ``missing_endpoints`` names endpoints that have no series matching the
    requested ``(data_type, name)`` at all — a whole-side gap, distinct from
    per-timestamp coverage. ``identities`` lists the (edge, endpoint, series)
    triples that fed the run so a skipped connection stays visible rather
    than implicit.
    """

    differences: pl.DataFrame
    coverage: pl.DataFrame
    identities: list[EndpointIdentity]
    missing_endpoints: pl.DataFrame

    def summary(self) -> dict[str, int | float | None]:
        """Headline counts for quick console/log consumption."""
        n_diff = self.differences.height
        return {
            "edges_reconciled": len({i.edge_uuid for i in self.identities}),
            "paired_points": n_diff,
            "mismatched_points": int(self.differences.filter(pl.col("abs_diff") > 0).height)
            if n_diff
            else 0,
            "unpaired_points": self.coverage.height,
            "endpoints_missing_series": self.missing_endpoints.height,
            "max_abs_diff": float(self.differences["abs_diff"].max()) if n_diff else None,
        }


def _edge_endpoint_paths(client: edb.Client, edge) -> tuple[str, str]:
    """Resolve an edge's endpoints to their materialized node paths.

    Uses the edge's own ``from_node`` / ``to_node`` scopes, so the paths come
    from the live topology rather than any naming convention.
    """
    from_scope = client.get_edge(uuid=edge.id).from_node()
    to_scope = client.get_edge(uuid=edge.id).to_node()
    from_path = "/".join(from_scope.path())
    to_path = "/".join(to_scope.path())
    return from_path, to_path


def _read_endpoint_series(
    client: edb.Client,
    manifest_rows: list[dict],
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Read every endpoint series in one manifest call.

    Returns ``(data, missing)``. ``on_missing="skip"`` means an endpoint
    without the requested series lands in ``missing`` instead of raising, so
    one under-instrumented connection does not abort the whole report.
    """
    manifest = pl.DataFrame(manifest_rows)
    data, missing = client.read(manifest, on_missing="skip")
    return data, missing


def _frame_for(df: pl.DataFrame, path: str, data_type: str, name: str) -> pl.DataFrame:
    """Extract one endpoint's (valid_time, value) frame, sorted and de-duped.

    A duplicate ``valid_time`` would make the join fan out; keep the last
    write per timestamp (FLAT actuals are point-in-time values) and say so.
    """
    sub = df.filter(
        (pl.col("path") == path) & (pl.col("data_type") == data_type) & (pl.col("name") == name)
    ).select("valid_time", "value")
    return sub.unique(subset=["valid_time"], keep="last").sort("valid_time")


_DIFF_SCHEMA = {
    "edge_uuid": pl.Utf8,
    "edge_type": pl.Utf8,
    "edge_name": pl.Utf8,
    "valid_time": pl.Datetime(time_zone="UTC"),
    "from_path": pl.Utf8,
    "from_value": pl.Float64,
    "to_path": pl.Utf8,
    "to_value": pl.Float64,
    "diff": pl.Float64,
    "abs_diff": pl.Float64,
}

_COVERAGE_SCHEMA = {
    "edge_uuid": pl.Utf8,
    "edge_type": pl.Utf8,
    "edge_name": pl.Utf8,
    "valid_time": pl.Datetime(time_zone="UTC"),
    "side": pl.Utf8,
    "node_path": pl.Utf8,
    "value": pl.Float64,
}

_MISSING_SCHEMA = {
    "edge_uuid": pl.Utf8,
    "edge_type": pl.Utf8,
    "edge_name": pl.Utf8,
    "side": pl.Utf8,
    "node_path": pl.Utf8,
    "data_type": pl.Utf8,
    "series_name": pl.Utf8,
}


def reconcile_edges(
    client: edb.Client,
    *,
    edge_type: str | None = None,
    data_type: str = "actual",
    series_name: str = "flow",
) -> ReconciliationReport:
    """Reconcile the two endpoint flow series of every matching edge.

    Parameters
    ----------
    client:
        A connected synchronous :class:`energydb.Client`.
    edge_type:
        Restrict to edges of this type (e.g. ``"Line"``); ``None`` considers
        every edge in the topology.
    data_type / series_name:
        The series to compare at each endpoint. Both endpoints must carry a
        series with this ``(data_type, name)`` for the edge to be reconciled.
    """
    edges = client.query_edges(type=edge_type)

    # Resolve real endpoint paths and build one batched read manifest.
    endpoint_paths: dict[str, tuple[str, str]] = {}
    manifest_rows: list[dict] = []
    for edge in edges:
        from_path, to_path = _edge_endpoint_paths(client, edge)
        endpoint_paths[str(edge.id)] = (from_path, to_path)
        for path in (from_path, to_path):
            manifest_rows.append({"path": path, "data_type": data_type, "name": series_name})

    identities: list[EndpointIdentity] = []
    diff_frames: list[pl.DataFrame] = []
    coverage_frames: list[pl.DataFrame] = []
    missing_rows: list[dict] = []

    if not manifest_rows:
        return ReconciliationReport(
            pl.DataFrame(schema=_DIFF_SCHEMA),
            pl.DataFrame(schema=_COVERAGE_SCHEMA),
            identities,
            pl.DataFrame(schema=_MISSING_SCHEMA),
        )

    data, missing = _read_endpoint_series(client, manifest_rows)
    missing_paths = set(missing["path"].to_list()) if missing.height else set()

    for edge in edges:
        edge_uuid = str(edge.id)
        from_path, to_path = endpoint_paths[edge_uuid]

        # Record topology identity for both ends. An endpoint whose series is
        # absent (listed in missing_paths) still appears here and in
        # missing_endpoints so the skipped connection stays visible.
        for role, path in (("from", from_path), ("to", to_path)):
            identities.append(
                EndpointIdentity(
                    edge_uuid=edge_uuid,
                    edge_type=type(edge).__name__,
                    edge_name=edge.name,
                    node_path=path,
                    role=role,
                    data_type=data_type,
                    series_name=series_name,
                )
            )
            if path in missing_paths:
                missing_rows.append(
                    {
                        "edge_uuid": edge_uuid,
                        "edge_type": type(edge).__name__,
                        "edge_name": edge.name,
                        "side": role,
                        "node_path": path,
                        "data_type": data_type,
                        "series_name": series_name,
                    }
                )

        from_df = _frame_for(data, from_path, data_type, series_name)
        to_df = _frame_for(data, to_path, data_type, series_name)

        edge_meta_expr = [
            pl.lit(edge_uuid, dtype=pl.Utf8).alias("edge_uuid"),
            pl.lit(type(edge).__name__, dtype=pl.Utf8).alias("edge_type"),
            pl.lit(edge.name, dtype=pl.Utf8).alias("edge_name"),
        ]

        # Full outer join on valid_time: both-present rows get compared;
        # one-sided rows become coverage entries.
        joined = from_df.join(to_df, on="valid_time", how="full", coalesce=True, suffix="_to")
        paired = joined.filter(pl.col("value").is_not_null() & pl.col("value_to").is_not_null())
        if paired.height:
            diff_frames.append(
                paired.select(
                    [
                        *edge_meta_expr,
                        pl.col("valid_time"),
                        pl.lit(from_path, dtype=pl.Utf8).alias("from_path"),
                        pl.col("value").alias("from_value"),
                        pl.lit(to_path, dtype=pl.Utf8).alias("to_path"),
                        pl.col("value_to").alias("to_value"),
                        (pl.col("value") - pl.col("value_to")).alias("diff"),
                        (pl.col("value") - pl.col("value_to")).abs().alias("abs_diff"),
                    ]
                )
            )

        only_from = joined.filter(pl.col("value").is_not_null() & pl.col("value_to").is_null())
        if only_from.height:
            coverage_frames.append(
                only_from.select(
                    [
                        *edge_meta_expr,
                        pl.col("valid_time"),
                        pl.lit("from", dtype=pl.Utf8).alias("side"),
                        pl.lit(from_path, dtype=pl.Utf8).alias("node_path"),
                        pl.col("value"),
                    ]
                )
            )
        only_to = joined.filter(pl.col("value").is_null() & pl.col("value_to").is_not_null())
        if only_to.height:
            coverage_frames.append(
                only_to.select(
                    [
                        *edge_meta_expr,
                        pl.col("valid_time"),
                        pl.lit("to", dtype=pl.Utf8).alias("side"),
                        pl.lit(to_path, dtype=pl.Utf8).alias("node_path"),
                        pl.col("value_to").alias("value"),
                    ]
                )
            )

    differences = (
        pl.concat(diff_frames, how="diagonal_relaxed").sort(["edge_uuid", "valid_time"])
        if diff_frames
        else pl.DataFrame(schema=_DIFF_SCHEMA)
    )
    coverage = (
        pl.concat(coverage_frames, how="diagonal_relaxed").sort(["edge_uuid", "valid_time", "side"])
        if coverage_frames
        else pl.DataFrame(schema=_COVERAGE_SCHEMA)
    )
    missing_endpoints = (
        pl.DataFrame(missing_rows, schema=_MISSING_SCHEMA).sort(["edge_uuid", "side"])
        if missing_rows
        else pl.DataFrame(schema=_MISSING_SCHEMA)
    )

    return ReconciliationReport(differences, coverage, identities, missing_endpoints)
