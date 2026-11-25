# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Translate between a record and one modelling framework.

Each module here holds one framework's vocabulary, for example
`datarecord.tools.pypsa`. Import the module of the framework you use and
install its extra (`datarecord[pypsa]`). The rest of `datarecord` imports no
tool.

The names exported here are what a tool may rely on between releases, the
contract in `datarecord.tools.base` and the helpers a tool builds on. A change
to any of them gets a changelog entry.

Notes
-----
- [tools](https://energy-models.github.io/datarecord/reference/tools/)
- [the PyPSA tool](https://energy-models.github.io/datarecord/reference/tools/#the-pypsa-tool)
- [the package](https://energy-models.github.io/datarecord/development/package/)
"""

from datarecord.duck import (
    empty_relation,
    ex_all,
    fn,
    join_on,
    null_safe,
    scratch_table,
)
from datarecord.resolve.fold import Fold
from datarecord.schema.categories import matching, with_defaults
from datarecord.settle import uncovered
from datarecord.tools.base import (
    Attr,
    Requirements,
    Schema,
    Tool,
    UnsupportedRecordError,
)

__all__ = [
    "Attr",
    "Fold",
    "Requirements",
    "Schema",
    "Tool",
    "UnsupportedRecordError",
    "empty_relation",
    "ex_all",
    "fn",
    "join_on",
    "matching",
    "null_safe",
    "scratch_table",
    "uncovered",
    "with_defaults",
]
