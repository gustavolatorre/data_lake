"""Shared test fixtures for PySpark and sample data."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession for testing.

    Uses a single session for the entire test suite to avoid
    the overhead of starting/stopping Spark per test.
    """
    session = (
        SparkSession.builder.master("local[2]")
        .appName("data_lake_tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )
    yield session
    session.stop()


BREWERY_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("brewery_type", StringType(), True),
        StructField("address_1", StringType(), True),
        StructField("address_2", StringType(), True),
        StructField("address_3", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state_province", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("country", StringType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("phone", StringType(), True),
        StructField("website_url", StringType(), True),
        StructField("state", StringType(), True),
        StructField("street", StringType(), True),
    ]
)

SAMPLE_BREWERIES = [
    (
        "id-001",
        "(405) Brewing Co",
        "micro",
        "1716 Topeka St",
        None,
        None,
        "Norman",
        "Oklahoma",
        "73069",
        "United States",
        -97.468,
        35.257,
        "4058160490",
        "http://405brewing.com",
        "Oklahoma",
        "1716 Topeka St",
    ),
    (
        "id-002",
        "Test Brewpub",
        "brewpub",
        "100 Main St",
        None,
        None,
        "Austin",
        "Texas",
        "78745",
        "United States",
        -97.7,
        30.26,
        "5121234567",
        None,
        "Texas",
        "100 Main St",
    ),
    (
        "id-003",
        "Null State Brewery",
        "nano",
        "200 Oak Ave",
        None,
        None,
        "Unknown",
        None,
        "00000",
        "United States",
        None,
        None,
        None,
        None,
        None,
        "200 Oak Ave",
    ),
    (
        "id-004",
        "Kärnten Brauerei",
        "micro",
        "Hauptstr 1",
        None,
        None,
        "Klagenfurt",
        "Kärnten",
        "9020",
        "Austria",
        14.3,
        46.6,
        None,
        None,
        "Kärnten",
        "Hauptstr 1",
    ),
    (
        "id-005",
        "Duplicate Brewery",
        "large",
        "500 Elm St",
        None,
        None,
        "Portland",
        "Oregon",
        "97201",
        "United States",
        -122.6,
        45.5,
        None,
        "http://duplicate.com",
        "Oregon",
        "500 Elm St",
    ),
]


@pytest.fixture
def sample_df(spark):
    """Create a sample DataFrame with realistic brewery data."""
    return spark.createDataFrame(SAMPLE_BREWERIES, schema=BREWERY_SCHEMA)


@pytest.fixture
def empty_df(spark):
    """Create an empty DataFrame with the brewery schema."""
    return spark.createDataFrame([], schema=BREWERY_SCHEMA)
