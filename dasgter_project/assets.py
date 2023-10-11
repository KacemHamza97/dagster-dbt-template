import time
import pandas as pd
import requests

from datetime import datetime
from dagster import AssetExecutionContext, EnvVar, file_relative_path, asset, OpExecutionContext
from . import constants
from .resources import CustomDagsterDbtTranslator, SlingResource, dbt_manifest_path
from dagster_dbt import dbt_assets, DbtCliResource
from dagster_duckdb import DuckDBResource
from typing import List, Tuple


def download_csv(context: AssetExecutionContext, url: str) -> Tuple[bool, float]:
    """
    Download a CSV file from the given URL and save it locally, and return the time elapsed.

    Args:
        url (str): The URL of the CSV file to download.
        local_file_path (str): The local file path where the CSV file will be saved.

    Returns:
        bool: True if the download was successful, False otherwise.
        float: The time elapsed in seconds.
    """
    try:
        start_time = time.time()  # Record the start time

        # Send an HTTP GET request to the URL
        context.log.info("Downloading data from {}".format(url))
        response = requests.get(url)

        # Check if the request was successful (HTTP status code 200)
        if response.status_code == 200:
            # Open the local file in binary write mode and write the content of the response to it
            context.log.info(
                "Downloaded {} bytes".format(len(response.content)))
            with open(file_relative_path(__file__, constants.EVP_FPATH), 'wb') as file:
                file.write(response.content)

            end_time = time.time()  # Record the end time

            print(f"CSV file downloaded and saved as {constants.EVP_FPATH}")
            return True, end_time - start_time  # Return success and time elapsed
        else:
            print(
                f"Failed to download CSV file. Status code: {response.status_code}")
            return False, None
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return False, None


@asset(compute_kind="python", group_name="Raw_data")
def electric_vehicle_population_raw(context: AssetExecutionContext):
    succ, elapsed_times = download_csv(context, constants.EVP_LINK)
    context.add_output_metadata(
        metadata={
            "Download was successful": succ,
            "elapsed_time": elapsed_times,
        },
    )


@asset(
    deps=[electric_vehicle_population_raw], compute_kind="duckdb", group_name="Transform Data"
)
def vehicle_information(context: AssetExecutionContext, duckdb: DuckDBResource):
    f_path = file_relative_path(__file__, constants.EVP_FPATH)
    new_cols = ['id', 'County', 'City', 'State', 'Postal_Code', 'Model_Year',
                'Make', 'Model', 'Electric_Vehicle_Type', 'CAFV_Eligibility', 'Electric_Range',
                'Base_MSRP', 'Legislative_District', 'DOL_Vehicle_ID', 'Vehicle_Location', 'Electric_Utility',
                'Census_Tract']
    cols = ['id', 'Model_Year', 'Make', 'Model', 'Electric_Vehicle_Type', 'CAFV_Eligibility',
            'Electric_Range', 'Base_MSRP', 'DOL_Vehicle_ID', 'Electric_Utility']
    df = pd.read_csv(f_path, header=0, names=new_cols)
    df_final = df[cols]
    df.dropna(inplace=True)
    df.reset_index(inplace=True)
    with duckdb.get_connection() as conn:
        conn.execute(
            "CREATE OR REPLACE TABLE vehicle_information AS SELECT * FROM df_final")
        nrows = conn.execute(
            "SELECT COUNT(*) FROM vehicle_information").fetchone()[0]  # type: ignore

        metadata = conn.execute(
            "select * from duckdb_tables() where table_name = 'vehicle_information'"
        ).pl()

    context.log.info("Created vehicle_information table")
    context.add_output_metadata(
        metadata={
            "num_rows": nrows,
            "table_name": metadata["table_name"][0],
            "datbase_name": metadata["database_name"][0],
            "schema_name": metadata["schema_name"][0],
            "column_count": metadata["column_count"][0],
            "estimated_size": metadata["estimated_size"][0],
        }
    )


@asset(
    deps=[electric_vehicle_population_raw], compute_kind="duckdb", group_name="Transform Data"
)
def location_information(context: AssetExecutionContext, duckdb: DuckDBResource):
    f_path = file_relative_path(__file__, constants.EVP_FPATH)
    new_cols = ['id', 'County', 'City', 'State', 'Postal_Code', 'Model_Year',
                'Make', 'Model', 'Electric_Vehicle_Type', 'CAFV_Eligibility', 'Electric_Range',
                'Base_MSRP', 'Legislative_District', 'DOL_Vehicle_ID', 'Vehicle_Location', 'Electric_Utility',
                'Census_Tract']
    cols = ['id', 'County', 'City', 'State', 'Postal_Code',
            'Legislative_District', 'Vehicle_Location', 'Census_Tract']
    df = pd.read_csv(f_path, header=0, names=new_cols)
    df_final = df[cols]
    df.dropna(inplace=True)
    df.reset_index(inplace=True)
    with duckdb.get_connection() as conn:
        conn.execute(
            "CREATE OR REPLACE TABLE location_information AS SELECT * FROM df_final")
        nrows = conn.execute(
            "SELECT COUNT(*) FROM location_information").fetchone()[0]  # type: ignore

        metadata = conn.execute(
            "select * from duckdb_tables() where table_name = 'location_information'"
        ).pl()

    context.log.info("Created location_information table")
    context.add_output_metadata(
        metadata={
            "num_rows": nrows,
            "table_name": metadata["table_name"][0],
            "datbase_name": metadata["database_name"][0],
            "schema_name": metadata["schema_name"][0],
            "column_count": metadata["column_count"][0],
            "estimated_size": metadata["estimated_size"][0],
        }
    )


@asset(group_name="Raw_data", compute_kind="Sling")
def film_raw(context: AssetExecutionContext, sling: SlingResource):
    fpath = file_relative_path(__file__, "../data/raw/film.csv")
    fsize, nrows = sling.sync(source_table="film", destination_file=fpath)
    context.add_output_metadata(
        metadata={"num_rows": nrows, "file_size": fsize, "path": fpath}
    )


@asset(group_name="Raw_data", compute_kind="Sling")
def category_raw(context: AssetExecutionContext, sling: SlingResource):
    fpath = file_relative_path(__file__, "../data/raw/category.csv")
    fsize, nrows = sling.sync(source_table="category", destination_file=fpath)
    context.add_output_metadata(
        metadata={"num_rows": nrows, "file_size": fsize, "path": fpath}
    )


@asset(group_name="Raw_data", compute_kind="Sling")
def film_category_raw(context: AssetExecutionContext, sling: SlingResource):
    fpath = file_relative_path(__file__, "../data/raw/film_category.csv")
    fsize, nrows = sling.sync(
        source_table="film_category", destination_file=fpath)
    context.add_output_metadata(
        metadata={"num_rows": nrows, "file_size": fsize, "path": fpath}
    )


@asset(deps=[film_raw], compute_kind="duckdb", group_name="Transform Data")
def film(context: AssetExecutionContext, duckdb: DuckDBResource):
    fpath = file_relative_path(__file__, "../data/raw/film.csv")
    with duckdb.get_connection() as conn:
        conn.execute(
            f"""CREATE OR REPLACE TABLE film AS (
                    SELECT * FROM read_csv_auto('{fpath}'))
            """
        )
        nrows = conn.execute(
            "SELECT COUNT(*) FROM film").fetchone()[0]  # type: ignore

        metadata = conn.execute(
            "select * from duckdb_tables() where table_name = 'film'"
        ).pl()

    context.add_output_metadata(
        metadata={
            "num_rows": nrows,
            "table_name": metadata["table_name"][0],
            "datbase_name": metadata["database_name"][0],
            "schema_name": metadata["schema_name"][0],
            "column_count": metadata["column_count"][0],
            "estimated_size": metadata["estimated_size"][0],
        }
    )

    context.log.info("Created tickets table")


@asset(deps=[category_raw], compute_kind="duckdb", group_name="Transform Data")
def category(context: AssetExecutionContext, duckdb: DuckDBResource):
    fpath = file_relative_path(__file__, "../data/raw/category.csv")
    with duckdb.get_connection() as conn:
        conn.execute(
            f"""CREATE OR REPLACE TABLE category AS (
                    SELECT * FROM read_csv_auto('{fpath}'))
            """
        )
        nrows = conn.execute(
            "SELECT COUNT(*) FROM category").fetchone()[0]  # type: ignore

        metadata = conn.execute(
            "select * from duckdb_tables() where table_name = 'category'"
        ).pl()

    context.add_output_metadata(
        metadata={
            "num_rows": nrows,
            "table_name": metadata["table_name"][0],
            "datbase_name": metadata["database_name"][0],
            "schema_name": metadata["schema_name"][0],
            "column_count": metadata["column_count"][0],
            "estimated_size": metadata["estimated_size"][0],
        }
    )

    context.log.info("Created events table")


@asset(deps=[film_category_raw], compute_kind="duckdb", group_name="Transform Data")
def film_category(context: AssetExecutionContext, duckdb: DuckDBResource):
    fpath = file_relative_path(__file__, "../data/raw/film_category.csv")
    with duckdb.get_connection() as conn:
        conn.execute(
            f"""CREATE OR REPLACE TABLE film_category AS (
                    SELECT * FROM read_csv_auto('{fpath}'))
            """
        )
        nrows = conn.execute(
            "SELECT COUNT(*) FROM film_category").fetchone()[0]  # type: ignore

        metadata = conn.execute(
            "select * from duckdb_tables() where table_name = 'film_category'"
        ).pl()

    context.add_output_metadata(
        metadata={
            "num_rows": nrows,
            "table_name": metadata["table_name"][0],
            "datbase_name": metadata["database_name"][0],
            "schema_name": metadata["schema_name"][0],
            "column_count": metadata["column_count"][0],
            "estimated_size": metadata["estimated_size"][0],
        }
    )

    context.log.info("Created events table")


@dbt_assets(manifest=dbt_manifest_path, dagster_dbt_translator=CustomDagsterDbtTranslator())
def dbt_birds(context: OpExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
