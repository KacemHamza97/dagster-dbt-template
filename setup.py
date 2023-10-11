from setuptools import find_packages, setup

setup(
    name="dasgter_project",
    packages=find_packages(exclude=["dasgter_project_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-dbt",
        "dagster-duckdb",
        "dbt-duckdb",
        "pandas",
        "polars",
        "pyarrow",
        "sling",
        "urllib3==1.26.15",
        "requests-toolbelt==0.10.1"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
