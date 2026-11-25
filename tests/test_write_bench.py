# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Writing and reading back `p` over 5000 entities and 2190 snapshots, about 11 million rows.

The record has three types, `Generator`, `Load` and `Link`. Its entities and
snapshots are committed as the base, outside the timing, so `set` and `commit`
time only the attribute writes: `p` over entity and snapshot, and the static
`p_nom`. The staged record is then written as a standalone directory, which is
read back once through a one-type entity filter and once in full.

Each repeat runs in its own spawned process, since `ru_maxrss` only rises.
Peak RSS is the high-water mark after each step, so a step that stays under an
earlier step's peak shows no increase. It is reported, not asserted, as is how
many row groups of the written `p` an entity filter could skip.

Notes
-----
- [writing a whole record](https://energy-models.github.io/datarecord/reference/layers/)
"""

import multiprocessing
import os
import resource
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from time import perf_counter

import duckdb
import pytest

from datarecord import AttributeSpec, WorkingRecord, Workspace
from tests.fixtures import empty_root, schema

TYPES = ("Generator", "Load", "Link")
ENTITIES = 5000
STEPS = 2190
REPEATS = 3
# A repeat started above this 1-minute load average is run once more.
BUSY = 8

# Seconds, about 1.5 times the minimum measured on the build host, rounded up.
BUDGETS = {
    "set": 10.0,
    "write": 4.0,
    "commit": 2.0,
    "read_filtered": 0.6,
    "read_full": 0.4,
}


def entities() -> dict[str, list[str]]:
    """The entity names of each type, the types splitting `ENTITIES` evenly."""
    names: dict[str, list[str]] = {t: [] for t in TYPES}
    for i in range(ENTITIES):
        ctype = TYPES[i % len(TYPES)]
        names[ctype].append(f"{ctype}{i:04d}")
    return names


def prunable(path: Path, filter: list[str]) -> tuple[int, int]:
    """How many of the row groups of `path` hold no entity of `filter`, and how many there are.

    A row group is prunable when its `entity` min-max statistics exclude every
    label of the filter; one without statistics never is.
    """
    con = duckdb.connect()
    stats = con.execute(
        "SELECT stats_min_value, stats_max_value FROM parquet_metadata(?)"
        " WHERE path_in_schema = 'entity'",
        [str(path)],
    ).fetchall()
    con.close()
    skipped = sum(
        low is not None
        and high is not None
        and all(name < low or name > high for name in filter)
        for low, high in stats
    )
    return skipped, len(stats)


def _peak_mb() -> float:
    """This process's `ru_maxrss` high-water mark, in MB: bytes on darwin, KB on Linux."""
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak / 2**20 if sys.platform == "darwin" else peak / 2**10


def repeat(root: str) -> dict:
    """One repeat in a fresh process: per-step seconds and peak RSS, start load, pruning lines."""
    load = os.getloadavg()
    names = entities()
    snapshots = [datetime(2013, 1, 1) + timedelta(hours=h) for h in range(STEPS)]
    over = {
        "p": AttributeSpec(dtype="float", dims={"entity", "snapshot"}),
        "p_nom": AttributeSpec(dtype="float", dims={"entity"}),
    }
    declared = schema(attributes={t: over for t in TYPES})
    ws = Workspace()
    store = ws.store(str(Path(root) / "store"))
    with WorkingRecord(empty_root(store, declared).commit().record) as w:
        for ctype, labels in names.items():
            w.add("entity", labels, ctype=ctype)
        w.add("snapshot", snapshots)
        base = store.commit(w)

    seconds, peak = {}, {}

    def timed(step: str, fn):
        start = perf_counter()
        result = fn()
        seconds[step] = perf_counter() - start
        peak[step] = _peak_mb()
        return result

    # `entities()` in SQL: DuckDB lists index from 1.
    entity = f"{list(TYPES)}[i % {len(TYPES)} + 1] || lpad(i::VARCHAR, 4, '0')"
    out = str(Path(root) / "written")
    with WorkingRecord(base.record) as w:
        rows = f"FROM range({ENTITIES}) e(i)"

        def stage():
            w.set(
                "p",
                w.con.sql(
                    f"SELECT {entity} AS entity,"
                    f" TIMESTAMP '2013-01-01' + to_hours(h) AS snapshot,"
                    f" (i * h % 7)::DOUBLE AS value {rows}, range({STEPS}) s(h)"
                ),
            )
            w.set(
                "p_nom",
                w.con.sql(f"SELECT {entity} AS entity, i::DOUBLE AS value {rows}"),
            )

        timed("set", stage)
        timed("write", lambda: ws.write(w, out))
        timed("commit", lambda: store.commit(w))

    record = ws.at(out)
    one = names[TYPES[0]]
    filtered = timed(
        "read_filtered",
        lambda: record.select(entity=one).attributes["p"].fetch_arrow_table(),
    )
    full = timed("read_full", lambda: record.attributes["p"].fetch_arrow_table())
    assert filtered.num_rows == len(one) * STEPS, "the filtered read keeps one type"
    assert full.num_rows == ENTITIES * STEPS, "the full read keeps every row"

    written = Path(out) / "attributes" / "p.parquet"
    pruning = []
    for label, filter in ((f"type {TYPES[0]}", one), (f"entity {one[0]}", one[:1])):
        k, n = prunable(written, filter)
        pruning.append(f"entity filter {label}: {k} of {n} row groups prunable")
    ws.close()
    return {"seconds": seconds, "peak": peak, "load": load, "pruning": pruning}


def _send(end, root: str) -> None:
    """Run `repeat` in the child and send its result back over `end`."""
    end.send(repeat(root))
    end.close()


def run(context, root: Path, i: int | str) -> dict:
    """Repeat `i` in a fresh spawned process, under its own directory.

    A bare process and a pipe rather than a pool, which needs a semaphore some
    sandboxes refuse.
    """
    receive, send = context.Pipe(duplex=False)
    child = context.Process(target=_send, args=(send, str(root / f"repeat-{i}")))
    child.start()
    send.close()
    try:
        result = receive.recv()
    except EOFError:
        result = None
    child.join()
    assert result is not None, f"repeat {i} exited with {child.exitcode}"
    return result


@pytest.mark.benchmark
def test_write_and_read_back_stay_within_budget():
    context = multiprocessing.get_context("spawn")
    with tempfile.TemporaryDirectory() as root:
        results = []
        for i in range(REPEATS):
            result = run(context, Path(root), i)
            if result["load"][0] > BUSY:
                result = run(context, Path(root), f"{i}-again")
            results.append(result)

    print(f"\n{ENTITIES} entities x {STEPS} snapshots, {ENTITIES * STEPS} rows of p")
    print(
        "load averages at start: "
        + "; ".join(", ".join(f"{x:.1f}" for x in r["load"]) for r in results)
    )
    fastest = {}
    for step in BUDGETS:
        timings = [r["seconds"][step] for r in results]
        fastest[step] = min(timings)
        peak = max(r["peak"][step] for r in results)
        print(
            f"{step}: {', '.join(f'{t:.3f}s' for t in timings)}"
            f" (min {fastest[step]:.3f}s), peak RSS {peak:.0f} MB"
        )
    for line in results[-1]["pruning"]:
        print(line)

    for step, budget in BUDGETS.items():
        assert fastest[step] <= budget, (
            f"{step} of {ENTITIES * STEPS} rows of p takes at most {budget:g} s,"
            f" took {fastest[step]:.3f}s"
        )
