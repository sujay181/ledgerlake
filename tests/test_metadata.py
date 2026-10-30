from datetime import datetime

import pytest

from ledgerlake import metadata
from ledgerlake.registry import PARAMETERS, REGISTRY

CUSTOMERS = {"table_id": 2, "table_name": "customers", "source_system": "sqlserver",
             "source_object": "core.customers", "target_layer": "silver"}


def test_parse_param_json_and_python_literal():
    assert metadata.parse_param('{"a": 1}') == {"a": 1}
    assert metadata.parse_param("{'a': 'x'}") == {"a": "x"}
    assert metadata.parse_param([1, 2]) == [1, 2]
    with pytest.raises(ValueError):
        metadata.parse_param("  ")


@pytest.mark.parametrize("bad", ["", "1abc", "a b", "x;DROP TABLE y", "t--", "a.b"])
def test_check_identifier_rejects_unsafe_names(bad):
    with pytest.raises(ValueError):
        metadata.check_identifier(bad)


def test_build_table_config_valid():
    cfg = metadata.build_table_config(CUSTOMERS, PARAMETERS[2])
    assert cfg.load_type == "MERGE" and cfg.primary_keys == ("customer_id",)
    assert cfg.is_incremental


def test_build_table_config_rejects_bad_configs():
    with pytest.raises(ValueError):
        metadata.build_table_config(CUSTOMERS, {"load_type": "UPSERT", "primary_key": "customer_id"})
    with pytest.raises(ValueError):
        metadata.build_table_config(CUSTOMERS, {"load_type": "MERGE", "primary_key": "customer_id"})
    with pytest.raises(ValueError):
        metadata.build_table_config(CUSTOMERS, {"load_type": "FULL"})
    with pytest.raises(ValueError):
        metadata.build_table_config({**CUSTOMERS, "source_system": "oracle"}, PARAMETERS[2])
    with pytest.raises(ValueError):
        metadata.build_table_config({**CUSTOMERS, "source_object": "core.customers; DROP"}, PARAMETERS[2])


def test_retry_on_conflict_retries_only_conflicts():
    calls = {"n": 0}

    class ConcurrentAppendException(Exception):
        pass

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise ConcurrentAppendException("conflict")
        return "ok"

    assert metadata.retry_on_conflict(flaky, base_delay=0) == "ok"
    assert calls["n"] == 3
    with pytest.raises(KeyError):
        metadata.retry_on_conflict(lambda: {}["missing"], base_delay=0)


def test_registry_row_to_dict_is_json_safe():
    import json

    d = metadata.registry_row_to_dict({"table_id": 1, "created_at": datetime(2026, 1, 1)})
    assert json.loads(json.dumps(d))["created_at"] == "2026-01-01T00:00:00"


def test_composite_keys():
    assert metadata.split_keys("a, b") == ["a", "b"]
    assert metadata.split_keys(None) == []


def test_every_silver_table_has_a_valid_config():
    ids = [r[0] for r in REGISTRY]
    assert len(ids) == len(set(ids))
    for tid, name, source, obj, layer, _order, _desc in REGISTRY:
        if layer == "silver":
            cfg = metadata.build_table_config(
                {"table_id": tid, "table_name": name, "source_system": source,
                 "source_object": obj, "target_layer": layer}, PARAMETERS[tid])
            assert cfg.primary_keys
