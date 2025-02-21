import logging
import logging.config
import os
import env_var 

logging.config.fileConfig('logging.config')
logger = logging.getLogger('ingest')
logger.setLevel(logging.DEBUG)


def get_src_path(in_path, in_file_type):
    """Retrieves the path and data files based on the file type from the data directory.
    Args:
        in_path (str): The path to the data directory.
        in_file_type (str): The type of file to retrieve ('parquet' or 'csv').
    Returns:
        tuple: A tuple containing file format, header, inferSchema, and file path."""
    try:
        logging.warning(f"Retrieving the file name from the path -> {in_path}")
        for file_nm in os.listdir(in_path):
            out_file_format = in_file_type
            if file_nm.endswith('.parquet') and in_file_type == 'parquet':
                out_header = 'False'
                out_inferschema = env_var.inferSchema
                out_path_file = os.path.join(in_path, file_nm)  # Use os.path.join
                logging.warning("Successfully retrieved the file and path -> {}".format(out_path_file))
            elif file_nm.endswith('.csv') and in_file_type == 'csv':
                out_header = env_var.header
                out_inferschema = env_var.inferSchema
                out_path_file = os.path.join(in_path, file_nm)  # Use os.path.join
                logging.warning(f"Successfully retrieved the file and path -> {out_path_file}")
    except Exception as err:
        logger.error('An error occurred while retrieving the path and file names ===> %s', str(err)) #use placeholders
        raise #re-raise the exception after logging.
    else:
        logging.warning(f"Path information has been read successfully of {out_file_format}")
    return out_file_format, out_header, out_inferschema, out_path_file


def ingest_data(in_spark, in_file_path, in_file_format, in_header, in_schema):
    """Ingests data into a Spark DataFrame.
    Args:
        in_spark (SparkSession): The SparkSession object.
        in_file_path (str): The path to the file to ingest.
        in_file_format (str): The format of the file ('parquet' or 'csv').
        in_header (str): Whether the file has a header ('True' or 'False').
        in_schema (str): Whether to infer the schema ('True' or 'False').
    Returns:DataFrame: The loaded Spark DataFrame."""
    try:
        logger.warning('File loading has started...')
        if in_schema == 'True':
            out_df = in_spark.read.format(in_file_format).option('header', in_header).option('inferSchema', in_schema).load(in_file_path)
        else:
            out_df = in_spark.read.format(in_file_format).load(in_file_path)
        logger.warning(f'Total no.of records loaded into dataframe from file: {out_df.count()}')
    except Exception as exc:
        logger.error('An error occurred at ingest process... ===> %s', str(exc)) #use placeholders
        raise
    else:
        logger.warning('Ingest process completed and DataFrame (df) created successfully...\U0001f600')
    return out_df