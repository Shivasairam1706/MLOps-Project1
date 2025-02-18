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

bitcoin_col = {"Open time" : ("open_tm", TimestampType()),
"Open" : ("open", FloatType()),
"High" : ("high", FloatType()),
"Low" : ("low", FloatType()),
"Close" : ("close", FloatType()),
"Volume" : ("vol", FloatType()),
"Close time" : ("close_tm", TimestampType()),
"Quote asset volume" : ("quote_asset_vol", FloatType()),
"Number of trades" : ("no_of_trades", IntegerType()),
"Taker buy base asset volume" : ("taker_buy_base_asset_vol", FloatType()),
"Taker buy quote asset volume" : ("taker_buy_quote_asset_vol", FloatType()),
"Ignore" : ("ignore", IntegerType())
}

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