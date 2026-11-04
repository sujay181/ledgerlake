from pathlib import Path

import pytest

from ledgerlake import gold
from ledgerlake.registry import REGISTRY

ROOT = Path(__file__).resolve().parents[1]


def test_gold_render():
    sql = gold.render("SELECT * FROM {catalog}.silver.x;", "ledgerlake")
    assert sql == "SELECT * FROM ledgerlake.silver.x"
    with pytest.raises(ValueError):
        gold.render("SELECT 1", "bad catalog")
    with pytest.raises(ValueError):
        gold.render("SELECT * FROM {schema}.x", "ledgerlake")


def test_every_gold_table_has_a_sql_file():
    for _tid, name, _source, _obj, layer, _order, _desc in REGISTRY:
        if layer == "gold":
            assert (ROOT / "gold_sql" / f"{name}.sql").exists(), f"missing gold_sql/{name}.sql"
