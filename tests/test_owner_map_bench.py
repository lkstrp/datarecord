# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""One attribute read of a `WorkingRecord` whose base rows set two or four `partial` dimensions."""

from itertools import product
from time import perf_counter

import pandas as pd
import pytest

from datarecord import WorkingRecord
from datarecord.schema.schema import Schema
from tests.fixtures import empty_root

_TYPES = {"entity": "str", "snapshot": "int", "scenario": "str", "carrier": "str"}


def _labels(dim: str, n: int) -> list:
    return (
        list(range(n)) if _TYPES[dim] == "int" else [f"{dim[0]}{i}" for i in range(n)]
    )


def staged_over(store, sizes: dict[str, int]) -> WorkingRecord:
    """A `WorkingRecord` over a root holding `x` at every label of `sizes`'s dims, all `partial`, with two edits.

    The edits name two patterns, `entity` alone and `entity` with the second
    of `dims`, so each base row is kept, covered or left to the fold.
    """
    dims = list(sizes)
    labels = {d: _labels(d, n) for d, n in sizes.items()}
    schema = Schema(
        dimensions={d: {"dtype": t} for d, t in _TYPES.items()},
        attributes={"x": {"dtype": "float", "dims": dims}},
        partial=frozenset(dims),
    )
    staged = WorkingRecord(empty_root(store, schema).commit().record)
    for d in dims:
        staged.add(d, labels[d])
    rows = pd.DataFrame(product(*labels.values()), columns=dims)
    staged.set("x", rows.assign(value=1.0))
    staged = WorkingRecord(store.commit(staged).record)
    staged.set("x", 2.0, entity=labels["entity"][0])
    staged.set("x", 3.0, entity=labels["entity"][1], **{dims[1]: labels[dims[1]][0]})
    return staged


@pytest.mark.benchmark
@pytest.mark.parametrize(
    ("sizes", "bound"),
    [
        ({"entity": 200, "snapshot": 1000}, None),
        ({"entity": 200, "snapshot": 40, "scenario": 5, "carrier": 5}, 1.0),
    ],
    ids=["2 partial", "4 partial"],
)
def test_a_staged_read_over_partial_dims_is_bounded(store, sizes, bound):
    staged = staged_over(store, sizes)
    start = perf_counter()
    rows = staged.attributes["x"].fetchall()
    took = perf_counter() - start

    print(f"\n{len(sizes)} partial dims: {len(rows)} rows read in {took:.3f}s")
    if bound is not None:
        assert took <= bound, (
            f"a staged read over {len(sizes)} partial dims took {took:.3f}s"
        )
