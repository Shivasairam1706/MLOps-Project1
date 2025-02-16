# to ingest the file data into target/Sink

import logging
import logging.config

logging.config.fileConfig('logging.config')
logger = logging.getLogger('ingest')
logger.setLevel(logging.DEBUG)

def  ingest_data(spark,file_path,file_format,header,inferschema):
    try :
        logger.warning('File loading has started...')
        
        if file_format == 'parquet':
            df = spark.read.format(file_format).load(file_path)
        
        elif file_format == 'csv' :
            df = spark.read.format(file_format).option(header = header,inferschema = inferschema).load(file_path)
        # Displays the count of the newly loaded dataframe
        logger.warning('Total no.of records loaded into dataframe from file : ',df.count())
    
    except Exception as exc :
        logger.warning('An error occured at ingest process... ===> ', str(exc))
        raise
    
    else :
        logger.warning('Ingest process completed and DataFrame (df) created successfully...\U0001f600')
    
    return df



