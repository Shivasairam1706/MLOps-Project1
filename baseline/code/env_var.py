import os

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