import logging.config

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
        # PySpark follows lazy evaluation; collect() forces execution.
        current_date_str = str(output.collect())
        logger.warning(f"Validating spark object with current date: {current_date_str}") #Use placeholders
    except Exception as exp:
        logger.error("An error has occurred in get_current_date method ===> %s", str(exp)) #Use placeholders
        raise
    else:
        logger.warning("Validation done... We are good to proceed...\U0001f600")
