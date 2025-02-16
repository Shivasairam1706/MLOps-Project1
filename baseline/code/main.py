import sys
import os
import env_var
from spark_session import get_spark_object
from validate import get_current_date
import logging
import logging.config
from ingest import ingest_data

open('application.log', 'w').close() ## To reset the application.log for every notebook run

logging.config.fileConfig('logging.config')
logger = logging.getLogger('root')
logger.setLevel(logging.DEBUG)

# To call get_spark_object() fucntion from spark_session.py to creating a new spark session 
def SparkSession():
    try:
        # logging the main method for creating spark session
        logging.info('Creating Spark Session... SparkSession Method started')
        # logging the spark status
        logging.info('Calling spark object...')
        out_spark = get_spark_object(env_var.env,env_var.appName)
        # Validating the spark session
        logging.info('Validating the Spark object')
        # sample function to get the current date using the newly created spark session
        get_current_date(out_spark)
    except exception as err:
        logging.error("Unable to create spark session. An error occured... please check ===> ",str(exp))
        sys.exit(1) # Ouputs exit code 1 incase of an issue while creating SparkSession
    
    return out_spark # Returns the created spark session

# Function to retrieve the  path and data files based on the file type from data directory
def get_src_path(in_path,in_file_type):
    # To get all the files present in the "path" w.r.t file type
    try:
        logging.info("Retrieving the file name from the path -> {}".format(in_path))
        for file_nm in os.listdir(in_path):
            out_file_format = in_file_type
            # pulling the parquet files based on the file extension
            if file_nm.endswith('.parquet') and in_file_type == 'parquet':
                out_header = 'NA' 
                out_inferschema = 'NA'
                out_path_file = in_path + '/' + file_nm # Concat data directory path with parquet files
                logging.info("Successfully retrieved the file and path -> {}".format(out_path_file))
            # pulling the csv files based on the file extension
            elif file_nm.endswith('.csv') and in_file_type == 'csv':
                out_header = env_var.header
                out_inferschema = env_var.inferschema
                out_path_file = in_path + '/' + file_nm # Concat data directory path with csv files
                logging.info("Successfully retrieved the file and path -> {}".format(out_path_file))
                
    except Exception as err:
        logger.error('An error occured while retrieving the path and file names ===>', str(err))
    
    logging.info("Path information has be read successfully of {}".format(out_file_format))
    # return the file format and other file info
    return out_file_format, out_header, out_inferschema, out_path_file

if __name__ == '__main__':
    # creates spark session/object
    spark = SparkSession()
    logging.info('Application done..')
    # Reads the files from source path and assigns base parameters using the file_type
    file_format, header, inferschema, path_file = get_src_path(env_var.src_path,'csv')
    # Creates a dataframe using the ingest_data method of ingest.py file
    main_df = ingest_data(in_spark=spark,in_file_path=path_file,in_file_format=file_format,in_header=header,in_inferschema=inferschema)