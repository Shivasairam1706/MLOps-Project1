# preprocessing the data
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import logging.config

logging.config.fileConfig('logging.config')
logger = logging.getLogger('data_prep')
logger.setLevel(logging.DEBUG)


def clean_sort(in_spark,in_df):

    try:
        logger.warning('Comparing the dataframe columns with the perdefined columns')
        inital_col_nm = ['Open time','Open','High','Low','Close','Volume','Close time','Quote asset volume','Number of trades','Taker buy base asset volume','Taker buy quote asset volume','Ignore']
        upd_col_nm = ["Open", "High", "Low",  "Volume_scaled","Quote_asset_volume_scaled","Number_of_trades_scaled", "TimeOfDay", "DayOfWeek"]
        if set(in_df.columns).issubset(set(inital_col_nm)):
            # Feature Engineering (Example: You can add more relevant features)
            in_df = in_df.withColumn("TimeOfDay", hour(col("Open time")))  # Hour of the day
            in_df = in_df.withColumn("DayOfWeek", dayofweek(col("Open time"))) # Day of the week
            in_df = in_df.withColumn("Volume_scaled", col("Volume") / mean(col("Volume"))) # Scaled Volume
            in_df = in_df.withColumn("Quote_asset_volume_scaled", col("Quote asset volume") / median(col("Quote asset volume"))) #Scaled Quote asset Volume
            in_df = in_df.withColumn("Number_of_trades_scaled", col("Number of trades") / mean(col("Number of trades"))) #Scaled Number of trades
            # Sort
        
