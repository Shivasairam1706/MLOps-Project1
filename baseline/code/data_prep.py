from pyspark.sql.types import *
from pyspark.sql.functions import *
import logging
import logging.config

logging.config.fileConfig('logging.config')
logger = logging.getLogger('data_prep')
logger.setLevel(logging.DEBUG)


def renm_nd_chg_typ(in_df, col_map):
    """Renames columns and changes their data types in a PySpark DataFrame.
    Args:
        in_df (DataFrame): The input PySpark DataFrame.
        col_map (dict): A dictionary mapping old column names to tuples of (new column name, new data type).
    Returns:
        DataFrame: The modified PySpark DataFrame."""
    try:
        # Replace string representations of NULL, None, NaN, etc. with null value
        logger.warning("Replacing string representations of string values with null")
        # Efficiently replace null-like strings with null using a single select expression.
        null_like = ["NULL", "None", "NaN", "null", ""]
        in_df = in_df.select([when(col(c).isin(null_like), None).otherwise(col(c)).alias(c) for c in in_df.columns])
        logger.warning("Null-like string replacements completed.")
        # Rename and cast columns in a single pass using a list comprehension.
        for old_nm, (new_nm, new_type) in col_map.items():
            if old_nm in in_df.columns:
                in_df = in_df.withColumn(new_nm, in_df[old_nm].cast(new_type))
            else:
                logger.warning(f"Column {old_nm} not found in DataFrame. Skipping.") #Informative warning.
                continue
    except Exception as err:
        logger.error(f"Error during renaming/type casting: {err}")
        raise
    else:
        logger.warning(f"Columns successfully renamed and data types changed. DataFrame schema: {in_df.schema}")
    return in_df
