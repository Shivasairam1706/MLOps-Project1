import os
from pyspark.sql.types import *

# Set environment variables
os.environ['env'] = 'DEV'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'

# Read environment variables
header = os.getenv('header', 'True')
inferSchema = os.getenv('inferSchema', 'True')
env = os.getenv('env', 'DEV')

# App name for Spark environment/session
appName = 'MLOps-project'

# Get the current working directory
act_path = os.path.dirname(os.getcwd())

# Creating path to the source files
src_path = os.path.join(act_path, 'data')

# Pre-processed data path
prep_path = os.path.join(src_path, 'prep_data/')

# Global variable to check the environment
Environments = ('DEV', 'SIT', 'UAT')

# Dataframe schema as a dictionary - "original name": ("new name", "data type")
bitcoin_cols = {
    "Open time": ("open_tm", TimestampType()),
    "Open": ("open", DoubleType()),
    "High": ("high", DoubleType()),
    "Low": ("low", DoubleType()),
    "Close": ("close", DoubleType()),
    "Volume": ("vol", DoubleType()),
    "Close time": ("close_tm", TimestampType()),
    "Quote asset volume": ("quote_asset_vol", FloatType()),
    "Number of trades": ("no_of_trades", IntegerType()),
    "Taker buy base asset volume": ("taker_buy_base_asset_vol", FloatType()),
    "Taker buy quote asset volume": ("taker_buy_quote_asset_vol", FloatType()),
    "Ignore": ("ignore", IntegerType())
}