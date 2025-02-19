import findspark
findspark.init()  # Initialize Spark
from pyspark.sql import SparkSession
import logging.config
import validate
import env_var
import sys

logging.config.fileConfig('logging.config')
logger = logging.getLogger('spark_session')
logger.setLevel(logging.DEBUG)


def get_spark_object(in_env, in_appName):
    """Creates a SparkSession based on the environment.
    Args:
        in_env (str): The environment (e.g., "local", "prod").
        in_appName (str): The name of the Spark application.
    Returns:
        SparkSession: The created SparkSession object."""
    try:
        logger.info('get_spark_object method has started...')
        if in_env in env_var.Environments:
            master = 'local[*]' #Use all local cores
        else:
            master = 'yarn'  # Corrected to lowercase 'yarn'
        logger.info('master is {}'.format(master))
        spark = SparkSession.builder.master(master).appName(in_appName).getOrCreate()
    except Exception as exp:
        logger.error('An error occurred in get_spark_object method... please check ===> %s', str(exp))
        raise
    else:
        logger.info('Spark session/object created successfully...')
    return spark


def create_spark_session():
    """Creates and validates a SparkSession.
    Returns:
        SparkSession: The created and validated SparkSession object."""
    try:
        logging.info('Creating Spark Session... create_spark_session Method started')
        logging.info('Calling spark object...')
        out_spark = get_spark_object(env_var.env, env_var.appName)
        logging.info('Validating the Spark object')
        validate.get_current_date(out_spark)  # Assuming validate.py has get_current_date
    except Exception as err:
        logging.error("Unable to create spark session. An error occurred... please check ===> %s", str(err))
        sys.exit(1)
    else:
        logger.info('Spark session/object is validate')
    return out_spark

def stop_spark_session(in_spark):
    """Stops the given SparkSession.
    Args:
        spark (SparkSession): The SparkSession to stop."""
    if in_spark:
        try:
            logging.info("Stopping SparkSession...")
            in_spark.stop()
            logging.info("SparkSession stopped successfully.")
        except Exception as e:
            logging.error(f"Error stopping SparkSession: {e}")
    else:
        logging.warning("SparkSession is None or already stopped.")
