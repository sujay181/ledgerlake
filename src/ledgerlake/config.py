"""Names shared by every notebook. Change the catalog with the `catalog` job parameter."""

DEFAULT_CATALOG = "ledgerlake"

# Unity Catalog schemas
METADATA = "metadata"
SOURCE = "source"      # holds the landing volume
BRONZE = "bronze"
SILVER = "silver"
GOLD = "gold"
ALL_SCHEMAS = [METADATA, SOURCE, BRONZE, SILVER, GOLD]

LANDING_VOLUME = "landing"

# Databricks secret scope and keys
SECRET_SCOPE = "ledgerlake"
SQLSERVER_SECRET_KEY = "sqlserver-connection"   # JSON: host, port, database, user, password
SMTP_SECRET_KEY = "gmail-app-password"


def landing_root(catalog: str) -> str:
    return f"/Volumes/{catalog}/{SOURCE}/{LANDING_VOLUME}"


def checkpoint_path(catalog: str, table_name: str) -> str:
    return f"{landing_root(catalog)}/_system/checkpoints/{table_name}"


def schema_path(catalog: str, table_name: str) -> str:
    return f"{landing_root(catalog)}/_system/schemas/{table_name}"


def fqn(catalog: str, schema: str, table: str) -> str:
    return f"{catalog}.{schema}.{table}"
