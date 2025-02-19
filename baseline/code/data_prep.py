from pyspark.sql.types import *
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
        for old_nm, (new_nm, new_type) in col_map.items():
            if old_nm in in_df.columns:
                in_df = in_df.withColumn(new_nm, in_df[old_nm].cast(new_type))
                if old_nm != new_nm:
                    in_df = in_df.drop(old_nm)
            else:
                logger.warning(f"Column {old_nm} not found in DataFrame. Skipping.") #Informative warning.
                continue
        logger.warning(f"Columns successfully renamed and data types changed. DataFrame schema: {in_df.schema}")
    except Exception as err:
        logger.error(f"Error during renaming/type casting: {err}")
        raise
    return in_df