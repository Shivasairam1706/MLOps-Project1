import os
from pyspark.sql.types import *

os.environ['env'] = 'DEV'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'

header = os.environ['header']
inferSchema = os.environ['inferSchema']
env = os.environ['env']

# App_Name for Spark Enviroment/Session...
appName = 'MLOp-project'

# To get the currect working directory...
act_path = os.getcwd()[:os.getcwd().rfind('/')]

# Creating path to the source files...
src_path = act_path + '/data'

# gobal variable to check the environment
Environments = ('DEV','SIT','UAT')

# Dataframe Schema as a dictionary - "orignial name": ("new name","data type")
new_cols = {"Open time" : ("open_tm", "Timestamp"),
"Open" : ("open", "Float"),
"High" : ("high", "Float"),
"Low" : ("low", "Float"),
"Close" : ("close", "Float"),
"Volume" : ("vol", "Float"),
"Close time" : ("close_tm", "Timestamp"),
"Quote asset volume" : ("quote_asset_vol", "Float"),
"Number of trades" : ("no_of_trades", "Integer"),
"Taker buy base asset volume" : ("taker_buy_base_asset_vol", "Float"),
"Taker buy quote asset volume" : ("taker_buy_quote_asset_vol", "Float"),
"Ignore" : ("ignore", "Integer")
}