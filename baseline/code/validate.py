import logging.config
from pyspark.sql.functions import *

logging.config.fileConfig('logging.config')
logger = logging.getLogger('Validate')
logger.setLevel(logging.DEBUG)

def get_current_date(spark):
    """Validates the SparkSession by retrieving and logging the current date.
    Args:
        spark (SparkSession): The SparkSession object to validate."""
    try:
        logger.warning("Started the get_current_date method...")
        output = spark.sql("SELECT current_date()")  # Corrected SQL syntax
        current_date_str = str(output.collect())  # Collect forces execution
        logger.warning("Validating spark object with current date: %s", current_date_str)
    except Exception as exp:
        logger.error("An error has occurred in get_current_date method ===> %s", str(exp))
        raise
    else:
        logger.warning("Spark session validation completed successfully...\U0001f600")

def data_validation(in_df, primary_col: str):
    """Performs data validation checks on the given DataFrame.
    Args:
        in_df (DataFrame): The DataFrame to validate.
        primary_col (str): The primary column to check for duplicates.
    Returns:
        tuple: A tuple containing missing values and duplicate count."""
    try:
        logger.warning("Checking for missing values...")
        missing_values = in_df.select([count(when(col(c).isNull(), c)).alias(c) for c in in_df.columns]).collect()
        logger.warning("Count of missing values in the dataframe: %s", missing_values)

        logger.warning("Checking for duplicate values...")
        dup_count = in_df.groupBy(primary_col).count().filter(col("count") > 1).count()
        logger.warning("Number of duplicate values in the dataframe: %d", dup_count)
    except Exception as exp:
        logger.error("An error occurred during data validation ===> %s", str(exp))
        raise
    else:
        logger.warning("Data validation completed successfully...\U0001f600\U0001f600")
    return missing_values, dup_count