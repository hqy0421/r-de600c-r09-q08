"""Seed the test-bench edge topology and its two endpoint flow series.

Topology (a single connection whose two ends each carry a flow meter):

    TestBench/
      Meter-A (JunctionPoint-style GridNode, the "from" end)
      Meter-B (GridNode, the "to" end)

    Line 'bench-conn-1'  Meter-A -> Meter-B      (the connection)

Each meter owns one FLAT series: data_type="actual", name="flow", unit="MW".

The two series deliberately share a common window with small discrepancies
AND keep timestamps the other side never recorded, so the reconciliation
report has both paired rows (diff) and unpaired rows (coverage table).

Idempotent enough to re-run on a fresh schema: call client.create() first
(the runner does), and use fixed UUIDs so re-runs of this script alone fail
loudly instead of silently duplicating.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

import energydb as edb
import polars as pl

PORTFOLIO = "TestBench"
METER_A = "Meter-A"
METER_B = "Meter-B"
EDGE_TYPE = "Line"
EDGE_NAME = "bench-conn-1"
DATA_TYPE = "actual"
SERIES_NAME = "flow"
UNIT = "MW"

# Fixed UUIDs make the topology identity reproducible across runs/reports.
UUID_A = UUID("01920000-0000-7000-8000-000000000001")
UUID_B = UUID("01920000-0000-7000-8000-000000000002")

BASE = datetime(2026, 11, 20, 8, 0, tzinfo=UTC)
HOUR = timedelta(hours=1)


def _hourly(rows: list[tuple[int, float]]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "valid_time": [BASE + HOUR * i for i, _ in rows],
            "value": [v for _, v in rows],
        },
        schema={"valid_time": pl.Datetime(time_zone="UTC"), "value": pl.Float64},
    )


# Meter-A: covers hours 0..5. Meter-B covers hours 2..7.
# Common window = hours 2..5 (paired, with a deliberate mismatch each hour).
# A-only unpaired: hours 0,1.  B-only unpaired: hours 6,7.
FLOW_A = _hourly(
    [
        (0, 100.0),
        (1, 102.0),
        (2, 101.5),
        (3, 99.8),
        (4, 103.2),
        (5, 100.7),
    ]
)

FLOW_B = _hourly(
    [
        (2, 101.2),   # diff A-B = +0.3
        (3, 99.8),    # diff = 0.0 (agreement)
        (4, 104.0),   # diff = -0.8
        (5, 100.9),   # diff = -0.2
        (6, 98.4),
        (7, 97.9),
    ]
)


def seed(client: edb.Client) -> None:
    meter_a = edb.grid.Meter(name=METER_A, id=UUID_A)
    meter_b = edb.grid.Meter(name=METER_B, id=UUID_B)
    client.register_tree(edb.Portfolio(name=PORTFOLIO, members=[meter_a, meter_b]))

    client.create_edge(
        edb.grid.Line(
            name=EDGE_NAME,
            capacity=200.0,
            from_element=edb.Reference(meter_a),
            to_element=edb.Reference(meter_b),
        )
    )

    for meter in (METER_A, METER_B):
        client.get_node(PORTFOLIO, meter).register_series(
            name=SERIES_NAME,
            canonical_unit=UNIT,
            data_type=DATA_TYPE,
            timeseries_type="FLAT",
        )

    client.get_node(PORTFOLIO, METER_A).write(FLOW_A, data_type=DATA_TYPE, name=SERIES_NAME)
    client.get_node(PORTFOLIO, METER_B).write(FLOW_B, data_type=DATA_TYPE, name=SERIES_NAME)


def main() -> None:
    with edb.Client() as client:
        client.create()
        seed(client)
        edges = client.query_edges(type=EDGE_TYPE)
        print(f"seeded {len(edges)} edge(s):")
        for edge in edges:
            print(f"  {EDGE_TYPE} {edge.name!r}: {edge.from_element} -> {edge.to_element}")
        for meter in (METER_A, METER_B):
            n = client.get_node(PORTFOLIO, meter).read(data_type=DATA_TYPE, name=SERIES_NAME).height
            print(f"  {PORTFOLIO}/{meter} flow rows: {n}")


if __name__ == "__main__":
    main()
