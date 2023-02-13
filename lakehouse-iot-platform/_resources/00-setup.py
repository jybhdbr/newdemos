# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
#Empty value will try default: dbdemos with a fallback to hive_metastore
dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("db", "", "Database")

# COMMAND ----------

# MAGIC %run ./00-global-setup-bundle $reset_all_data=$reset_all_data $db_prefix=manufacturing $catalog=$catalog $db=$db

# COMMAND ----------

#Let's skip some warnings for cleaner output
import warnings
warnings.filterwarnings("ignore")
database = dbName

# COMMAND ----------

import json
import time
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
import pyspark.sql.functions as F

reset_all_data = dbutils.widgets.get("reset_all_data") == "true"
cloud_storage_path = cloud_storage_path + "/iot_wind_turbine"

import json
import time
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, sha1, col, initcap, to_timestamp
import pyspark.sql.functions as F

folder = '/demos/manufacturing/iot_turbine'

if reset_all_data:
  dbutils.fs.rm('/demos/manufacturing/iot_turbine', True)
if reset_all_data or is_folder_empty(folder+"/historical_turbine_status") or is_folder_empty(folder+"/parts") or is_folder_empty(folder+"/turbine") or is_folder_empty(folder+"/incoming_data"):
  #data generation on another notebook to avoid installing libraries (takes a few seconds to setup pip env)
  print(f"Generating data under {folder} , please wait a few sec...")
  path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
  parent_count = path[path.rfind("lakehouse-iot-platform"):].count('/') - 1
  prefix = "./" if parent_count == 0 else parent_count*"../"
  prefix = f'{prefix}_resources/'
  dbutils.notebook.run(prefix+"01-load-data", 600, {"root_folder": folder})
else:
  print("data already existing. Run with reset_all_data=true to force a data cleanup for your local demo.")

# COMMAND ----------

#cloud_storage_path = dbutils.widgets.get("cloud_storage_path")
from databricks.feature_store import FeatureStoreClient
fs = FeatureStoreClient()

def display_automl_turbine_maintenance_link(dataset, model_name, force_refresh = False): 
  if force_refresh:
    reset_automl_run("lakehouse_turbine_maintenance_auto_ml")
  display_automl_link("lakehouse_turbine_maintenance_auto_ml", model_name, dataset, "abnormal_sensor", 5, move_to_production=False)

def get_automl_turbine_maintenance_run(force_refresh = False): 
  if force_refresh:
    reset_automl_run("lakehouse_turbine_maintenance_auto_ml")
  from_cache, r = get_automl_run_or_start("lakehouse_turbine_maintenance_auto_ml", "dbdemos_turbine_maintenance", fs.read_table(f'{database}.turbine_hourly_features'), "abnormal_sensor", 5, move_to_production=False)
  return r
