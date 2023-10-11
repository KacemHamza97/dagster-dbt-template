from dagster import Definitions, EnvVar, load_assets_from_modules

from . import assets, resources

all_assets = load_assets_from_modules([assets])
duckdb_resource = resources.duckdb_resource

defs = Definitions(
    assets=all_assets,
    resources={
        "duckdb": duckdb_resource,
        "sling": resources.SlingResource(postgres_connect_str=EnvVar("PG_CONN")),
        "dbt": resources.dbt_resource,
    },
)
