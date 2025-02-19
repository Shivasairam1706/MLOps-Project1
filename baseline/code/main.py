import sys
import os
import env_var
import spark_session
import validate
import logging
import logging.config
import ingest
import data_prep

open('application.log', 'w').close() ## To reset the application.log for every notebook run

logging.config.fileConfig('logging.config')
logger = logging.getLogger('root')
logger.setLevel(logging.DEBUG)

if __name__ == '__main__':
    # creates spark session/object
    spark = spark_session.create_spark_session()
    logging.info('Application done..')
    # Reads the files from source path and assigns base parameters using the file_type
    file_format, header, inferschema, path_file = ingest.get_src_path(env_var.src_path,'csv')
    # Creates a dataframe using the ingest_data method of ingest.py file
    main_df = ingest.ingest_data(in_spark=spark,in_file_path=path_file,in_file_format=file_format,in_header=header,in_schema=inferschema)
    # Renaming the columns and changing datatypes
    main_df = data_prep.renm_nd_chg_typ(main_df, env_var.new_cols)
    # Stops the spark session/object
    spark_session.stop_spark_session(spark)