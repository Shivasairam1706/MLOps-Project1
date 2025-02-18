# preprocessing the data
from pyspark.sql.types import *
import logging
import logging.config

logging.config.fileConfig('logging.config')
logger = logging.getLogger('data_prep')
logger.setLevel(logging.DEBUG)

def renm_nd_chg_typ(in_df, col_map):
    """ Renames columns and changes their data types in a PySpark DataFrame.
    Args:
        df: The input PySpark DataFrame.
        col_map: A dictionary where keys are the old column names and values are tuples of (new_column_name, new_data_type).
    Returns:
        A new PySpark DataFrame with the renamed and type-casted columns, or the original DataFrame if no changes are needed.  Returns the original DataFrame if any errors occur (like a column not existing).  It's best to handle exceptions where you call this. """
    try:
        new_col = []
        for old_nm, (new_nm, new_type) in col_map.items():
            if old_nm in df.columns:  # Check if the column exists
                #new_col.append(df[old_nm].cast(new_type).alias(new_nm)) # Cast and alias
                in_df = in_df.withColumn("old_nm", in_df["old_nm"].cast("new_type").alias("new_nm"))
            else:
                logger.warning("Column {} not found in DataFrame.".format(old_nm))
                break # To raise exception
                
        '''if new_col: # Only create a new DataFrame if there are changes
            return df.select(*new_col) # Select the new columns (and any others)
        else:
            return df # No changes needed '''
    except Exception as err:
        logger.warning("Error during renaming/type casting: ", str(err))
    return in_df  # Return the original DataFrame in case of error

