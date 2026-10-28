import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "source_data"))


@pytest.fixture(scope="session")
def generated(tmp_path_factory):
    """A small generated dataset shared by the tests."""
    from generate_data import generate
    from datetime import date

    out = tmp_path_factory.mktemp("generated")
    counts = generate(out, seed=7, n_customers=150, history_start=date(2025, 10, 1),
                      history_days=60, incremental_days=7)
    return out, counts


@pytest.fixture(scope="session")
def spark():
    """Local Spark (no Delta Lake jars needed). Skipped when PySpark/Java aren't available."""
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    if not shutil.which("java") and not os.environ.get("JAVA_HOME"):
        pytest.skip("Java not available")
    warehouse = tempfile.mkdtemp(prefix="ll-warehouse-")
    session = (SparkSession.builder.master("local[2]").appName("ledgerlake-tests")
               .config("spark.sql.warehouse.dir", warehouse)
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.ansi.enabled", "true")
               .config("spark.sql.shuffle.partitions", "4")
               .config("spark.ui.enabled", "false")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)
