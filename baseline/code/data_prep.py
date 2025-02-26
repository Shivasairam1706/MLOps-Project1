from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import logging
import logging.config

logging.config.fileConfig('logging.config')
logger = logging.getLogger('data_prep')
logger.setLevel(logging.DEBUG)

def renm_nd_chg_typ(in_df, col_map: dict):
    """Renames columns and changes their data types in a PySpark DataFrame.
    Args:
        in_df (DataFrame): The input PySpark DataFrame.
        col_map (dict): A dictionary mapping old column names to tuples of (new column name, new data type).
    Returns:
        DataFrame: The modified PySpark DataFrame."""
    try:
        # Validate data types in col_map
        for _, (_, new_type) in col_map.items():
            if not isinstance(new_type, DataType):
                logger.error(f"Invalid data type: {new_type}")
                raise TypeError(f"Invalid data type: {new_type}")
        # Combine null-like string replacement and renaming/casting
        null_like = ["NULL", "None", "NaN", "null", ""]
        select_exprs = [when(col(c).isin(null_like), None)
                   .otherwise(col(c))
                   .cast(col_map[c][1])  # Data type from tuple
                   .alias(col_map[c][0])  # New name from tuple
                if c in col_map else col(c)
                for c in in_df.columns]
        out_df = in_df.select(select_exprs)
    except Exception as err:
        logger.error(f"Error during renaming/type casting: {err}")
        raise
    else:
        logger.warning(f"Columns successfully renamed and data types changed. DataFrame schema: {out_df.schema}")
    return out_df

def data_handling(in_df, date_column='open_tm', missing_strategy='drop', fill_value=0,
                  duplicate_strategy='first', rolling_period=4, rolling_col='close'):
    """Validates data for missing values and duplicates, and handles them based on specified strategies.
    Args:
        in_df (DataFrame): The input PySpark DataFrame.
        date_column (str): The primary column used to identify duplicates.
        missing_strategy (str, optional): Strategy for handling missing values ('drop', 'fill', etc.). Defaults to 'drop'.
        fill_value (any, optional): Value to fill missing data with when using 'fill'. Defaults to 0.
        duplicate_strategy (str, optional): Strategy for handling duplicates ('first', 'last', 'drop'). Defaults to 'first'.
        rolling_period (int, optional): Period for rolling average calculation. Defaults to 4.
        rolling_col (str, optional): Column used for rolling average calculation. Defaults to 'close'.
    Returns:
        DataFrame: The DataFrame after handling missing values and duplicates."""
    try:
        # Handle missing values based on strategy
        if missing_strategy == 'drop':
            logger.warning('Dropping rows with missing values.')
            in_df = in_df.dropna()
        elif missing_strategy == 'fill':
            logger.warning(f'Filling missing values with {fill_value}.')
            in_df = in_df.fillna(fill_value)
        else:
            logger.warning(f'Unknown missing value strategy: {missing_strategy}. No missing value handling performed.')
        # Handle duplicates based on strategy
        if duplicate_strategy == 'first':
            logger.warning('Removing duplicate rows, keeping the first occurrence.')
            in_df = in_df.dropDuplicates([date_column])
        elif duplicate_strategy == 'last':
            logger.warning('Removing duplicate rows, keeping the last occurrence.')
            window_spec = Window.partitionBy(date_column).orderBy(desc(date_column))
            in_df = in_df.withColumn('rank', row_number().over(window_spec)).filter(col('rank') == 1).drop('rank')
        elif duplicate_strategy == 'drop':
            logger.warning('Removing all duplicate rows.')
            in_df = in_df.dropDuplicates([date_column])
        else:
            logger.warning(f'Unknown duplicate strategy: {duplicate_strategy}. No duplicate handling performed.')
        # Rolling average for reducing noise and highlighting trends within a fixed window.
        logger.warning('Creating a time window for checking trends using rolling average.')
        window_spec = Window.orderBy(date_column).rowsBetween(-rolling_period, rolling_period)
        in_df = in_df.withColumn('rolling_avg', avg(col(rolling_col)).over(window_spec))
        logger.warning("Created a new column 'rolling_avg' for checking the trends within a fixed window.")
    except Exception as exp:
        logger.error(f'An error occurred during data validation and handling: {exp}')
        raise
    else:
        logger.warning('Data validation and handling completed successfully. 😊😊')
    return in_df

def data_Partition(in_df, date_column: str, partition_by: str):
    """Partitions a DataFrame based on a date column, dynamically choosing the partition granularity.
    Args:
        in_df : The input DataFrame.
        date_column (str): The name of the date column.
        partition_by (str): The partition granularity ("month", "quarter", "half-year", "year").
    Returns:
        DataFrame: The DataFrame with the partition column added."""
    try:
        logger.warning(f"DataFrame {partition_by} based partitioning has started...")
        # Sorts the data in chronological order
        in_df = in_df.orderBy(date_column)
        # Add partitioning column based on granularity
        if partition_by == "month":
            in_df = in_df.withColumn("month", month(in_df[date_column]))
        elif partition_by == "quarter":
            in_df = in_df.withColumn("quarter", quarter(in_df[date_column]))
        elif partition_by == "half-year":
            in_df = in_df.withColumn("half-year", (quarter(in_df[date_column]) + 1) // 2)
        elif partition_by == "year":
            in_df = in_df.withColumn("year", year(in_df[date_column]))
        else:
            raise ValueError(f"Invalid partition_by value: {partition_by}. Must be 'month', 'quarter', 'half-year' or 'year'.")
        # Check if the column exists in the DataFrame
        logger.warning(f"Columns in DataFrame: {in_df.columns}")
        if partition_by not in in_df.columns:
            raise Exception(f"Partition column '{partition_by}' not found in DataFrame!")
        # Repartition based on the created column
        out_df = in_df.repartition(in_df[partition_by])
        return out_df
    except Exception as e:
        logger.error(f"An error occurred while partitioning data: {e}")
        raise
    else:
        logger.warning('Data Partitioning completed successfully...')