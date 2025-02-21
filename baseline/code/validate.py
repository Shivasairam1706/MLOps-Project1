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
        logger.warning("Spark session validation completed successfully...\U0001f600")


def data_validation(in_df,primary_col):
    try:
        # Check for missing values in the dataframe
        logger.warning("Checking for Missing values...")
        missing_values = main_df.select([count(when(col(c).isNull(), c)).alias(c) for c in main_df.columns]).rdd.map(lambda row: (row[c], row[c])).collect()
        logger.warning(f"Below is the count of Missing values in the dataframe: \n{missing_values.show()}")
        logger.warning("Checking for duplciate values...")
        dup_count = df.groupBy(primary_col).count().filter(col("count") > 1).count()
        logger.warning(f"No.of duplicate values in the dataframe: {dup_count}")
    return missing_values,dup_count
    except Exception as exp:
        logger.error("An error has occurred while performing data_validation ===> %s", str(exp))
        raise
    else:
        logger.warning("Data Validation done... Successfully \U0001f600\U0001f600")
    





    