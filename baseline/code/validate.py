import logging.config

logging.config.fileConfig('logging.config')
logger = logging.getLogger('Validate')
logger.setLevel(logging.DEBUG)


# function the validate the spark session
def get_current_date(spark):
    try:
        logger.warning("started the get_current_date method...")
        output = spark.sql("""select Current_date""")
        # Pyspark is follows lazy evalation to check check the spark object is created or not we are using collect fuction to execute the spark object...
        logger.warning("Validating spark object with current date" + str(output.collect()))
    
    except Exception as exp:
        logger.error("An error has occured in get_current_date method ===>",str(exp))
        
        raise
        
    else :
        logger.warning("Validatation done... We are good to proceed...\U0001f600")