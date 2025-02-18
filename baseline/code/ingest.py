# to ingest the file data into target/Sink
import logging
import logging.config

logging.config.fileConfig('logging.config')
logger = logging.getLogger('ingest')
logger.setLevel(logging.DEBUG)

def ingest_data(in_spark,in_file_path,in_file_format,in_header,in_schema):
    try :
        logger.warning('File loading has started...')
        if in_schema == 'True':
            out_df = in_spark.read.format(in_file_format).option('header', in_header).option('inferSchema', in_schema).load(in_file_path)
        else:
            out_df = in_spark.read.format(in_file_format).load(in_file_path)
        # Displays the count of the newly loaded dataframe
        logger.warning('Total no.of records loaded into dataframe from file: {}'.format(out_df.count()))
    
    except Exception as exc :
        logger.warning('An error occured at ingest process... ===> ', str(exc))
        raise
    
    else :
        logger.warning('Ingest process completed and DataFrame (df) created successfully...\U0001f600')
    
    return out_df

