import logging
import logging.config
import os
import env_var
from delta.tables import DeltaTable

logging.config.fileConfig('logging.config')
logger = logging.getLogger('ingest')
logger.setLevel(logging.DEBUG)

def get_src_path(in_path: str, in_file_type: str):
    """Retrieves the path and data files based on the file type from the data directory."""
    try:
        logger.warning(f"Retrieving the file name from the path -> {in_path}")
        for file_nm in os.listdir(in_path):
            if file_nm.endswith(f'.{in_file_type}'):
                out_header = env_var.header if in_file_type == 'csv' else 'False'
                out_inferschema = env_var.inferSchema
                out_path_file = os.path.join(in_path, file_nm)
                logger.warning(f"Successfully retrieved the file and path -> {out_path_file}")
                return in_file_type, out_header, out_inferschema, out_path_file
        raise FileNotFoundError(f"No {in_file_type} files found in {in_path}")
    except Exception as err:
        logger.error('An error occurred while retrieving the path and file names ===> %s', str(err))
        raise


def read_data(in_spark, in_file_path: str, in_file_format='parquet', in_header='True', in_schema='False'):
    """Reads data into a Spark DataFrame."""
    try:
        logger.warning(f'Reading data from the {in_file_format} file.')
        read_options = {'header': in_header}
        if in_schema == 'True':
            read_options['inferSchema'] = 'True'
        out_df = in_spark.read.format(in_file_format).options(**read_options).load(in_file_path)
        logger.warning(f'Total no.of records loaded into dataframe from {in_file_format} file: {out_df.count()}')
        return out_df
    except Exception as exc:
        logger.error('An error occurred while reading data... ===> %s', str(exc))
        raise

def load_data(in_df, in_file_path, partitioned_by='', in_file_format='delta', in_mode='overwrite'):
    try:
        logger.warning('File loading has started...')
        write_options = in_df.write.format(in_file_format).mode(in_mode)
        if partitioned_by:
            write_options = write_options.partitionBy(partitioned_by)
        write_options.save(in_file_path)
    except Exception as err:
        logger.error('An error occurred while loading the data... ===> %s', str(err))
        raise
    else:
        logger.warning('File loading completed successfully...')


def create_delta_table(in_spark, table_name: str, delta_table_path: str) -> DeltaTable:
    try:
        logger.warning(f"Checking if Delta table '{table_name}' exists or not...")
        existing_properties = in_spark.sql(f"DESCRIBE EXTENDED delta.`{delta_table_path}`").collect()
        logger.warning(f"Existing table properties: {existing_properties}")
        in_spark.sql(f"""CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{delta_table_path}' """)
        in_spark.sql(f"""ALTER TABLE {table_name} SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true')""")
        delta_table = DeltaTable.forPath(in_spark, delta_table_path)
        in_spark.sql(f"DESCRIBE EXTENDED {table_name}").show()
        delta_table.history().show()
    except Exception as e:
        logger.error(f"Error creating Delta table: {e}")
        raise
    else:
        logger.warning(f"Delta table '{table_name}' created successfully.")
        return delta_table


def append_data_to_delta(in_spark, in_df, delta_table_path: str, date_column: str):
    """Appends new data to an existing Delta Lake table."""
    try:
        logger.warning("Checking whether the Delta table exists and appending new data...")
        delta_table = DeltaTable.forPath(in_spark, delta_table_path)
        delta_table.alias("oldData").merge(in_df.alias("newData"),f"oldData.{date_column} = newData.{date_column}").whenNotMatchedInsertAll().execute()
        # Optional: Vacuum to manage retention
        in_spark.sql(f"VACUUM {delta_table_path} RETAIN 168 HOURS")
        # Optional: Describe the table after changes
        in_spark.sql(f"DESCRIBE EXTENDED {delta_table_path}").show()
    except Exception as e:
        logger.error(f"Error appending data: {e}")
        raise
    else:
        logger.warning("New data appended successfully.")
        return delta_table