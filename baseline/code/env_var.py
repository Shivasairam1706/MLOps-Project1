import os

os.environ['env'] = 'DEV'
os.environ['header'] = 'True'
os.environ['InferSchema'] = 'True'

header = os.environ['header']
InferSchema = os.environ['InferSchema']
env = os.environ['env']

# App_Name for Spark Enviroment/Session...
appName = 'MLOp-project'

# To get the currect working directory...
act_path = os.getcwd()

# Creating path to the source files...
src_path = act_path + '\\data'