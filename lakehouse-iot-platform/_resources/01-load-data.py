# Databricks notebook source
# MAGIC %pip install git+https://github.com/QuentinAmbard/mandrova faker

# COMMAND ----------

dbutils.widgets.text('root_folder', '/demos/manufacturing/iot_turbine', 'Root Folder')
root_folder = dbutils.widgets.get('root_folder')

# COMMAND ----------

from mandrova import SensorDataGenerator as sdg
import numpy as np
import pandas as pd
import random
import time
import uuid
import pyspark.sql.functions as F


def generate_sensor_data(turbine_id, sensor_conf, faulty = False, sample_size = 1000, display_graph = True, noise = 2, delta = -3):
  dg = sdg()
  rd = random.Random()
  rd.seed(turbine_id)
  dg.seed(turbine_id)
  sigma = sensor_conf['sigma']
  #Faulty, change the sigma with random value
  if faulty:
    sigma *= rd.randint(8,20)/10
    
  dg.generation_input.add_option(sensor_names="normal", distribution="normal", mu=0, sigma = sigma)
  dg.generation_input.add_option(sensor_names="sin", eq=f"2*exp(sin(t))+{delta}", initial={"t":0}, step={"t":sensor_conf['sin_step']})
  dg.generate(sample_size)
  sensor_name = "sensor_"+ sensor_conf['name']
  dg.sum(sensors=["normal", "sin"], save_to=sensor_name)
  max_value = dg.data[sensor_name].max()
  min_value = dg.data[sensor_name].min()
  if faulty:
    n_outliers = int(sample_size*0.15)
    outliers = np.random.uniform(-max_value*rd.randint(2,3), max_value*rd.randint(2,3), n_outliers)
    indicies = np.sort(np.random.randint(0, sample_size-1, n_outliers))
    dg.inject(value=outliers, sensor=sensor_name, index=indicies)

  n_outliers = int(sample_size*0.01)
  outliers = np.random.uniform(min_value*noise, max_value*noise, n_outliers)
  indicies = np.sort(np.random.randint(0, sample_size-1, n_outliers))
  dg.inject(value=outliers, sensor=sensor_name, index=indicies)
  
  if display_graph:
    dg.plot_data(sensors=[sensor_name])
  return dg.data[sensor_name]

# COMMAND ----------

sensors = [{"name": "A", "sin_step": 0, "sigma": 1},
           {"name": "B", "sin_step": 0, "sigma": 2},
           {"name": "C", "sin_step": 0, "sigma": 3},
           {"name": "D", "sin_step": 0.1, "sigma": 1.5},
           {"name": "E", "sin_step": 0.01, "sigma": 2},
           {"name": "F", "sin_step": 0.2, "sigma": 1}]
current_time = int(time.time()) - 3600*30

#Sec between 2 metrics
frequency_sec = 10
#X points per turbine (1 point per frequency_sec second)
sample_size = 2125
turbine_count = 512
dfs = []

# COMMAND ----------

def generate_turbine_data(turbine):
  rd = random.Random()
  rd.seed(turbine)
  damaged = turbine > turbine_count*0.6
  if turbine % 10 == 0:
    print(f"generating turbine {turbine} - damage: {damaged}")
  df = pd.DataFrame()
  damaged_sensors = []
  rd.shuffle(sensors)
  for s in sensors:
    #30% change to have 1 sensor being damaged
    #Only 1 sensor can send damaged metrics at a time to simplify the model. A C and E won't be damaged for simplification
    if damaged and len(damaged_sensors) == 0 and s['name'] not in ["A", "C", "E"]:
      damaged_sensor = rd.randint(1,10) > 5
    else:
      damaged_sensor = False
    if damaged_sensor:
      damaged_sensors.append('sensor_'+s['name'])
    plot = turbine == 0
    df['sensor_'+s['name']] = generate_sensor_data(turbine, s, damaged_sensor, sample_size, plot)

  dg = sdg()
  #Damaged turbine will produce less
  factor = 50 if damaged else 30
  energy = dg.generation_input.add_option(sensor_names="energy", eq="x", initial={"x":0}, step={"x":np.absolute(np.random.randn(sample_size).cumsum()/factor)})
  dg.generate(sample_size, seed=rd.uniform(0,10000))
  #Add some null values in some timeseries to get expectation metrics
  if damaged and rd.randint(0,9) >7:
    n_nulls = int(sample_size*0.005)
    indicies = np.sort(np.random.randint(0, sample_size-1, n_nulls))
    dg.inject(value=None, sensor="energy", index=indicies)

  if plot:
    dg.plot_data()
  df['energy'] = dg.data['energy']

  df.insert(0, 'timestamp', range(current_time, current_time + len(df)*frequency_sec, frequency_sec))
  df['turbine_id'] = str(uuid.UUID(int=rd.getrandbits(128)))
  #df['damaged'] = damaged
  df['abnormal_sensor'] = "ok" if len(damaged_sensors) == 0 else damaged_sensors[0]
  return df

from typing import Iterator
import pandas as pd
from pyspark.sql.functions import pandas_udf, col  

df_schema=spark.createDataFrame(generate_turbine_data(0)) 

def generate_turbine(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
  for pdf in iterator:
    for i, row in pdf.iterrows():
      yield generate_turbine_data(row["id"])

spark_df = spark.range(0, turbine_count).repartition(int(turbine_count/10)).mapInPandas(generate_turbine, schema=df_schema.schema)
spark_df = spark_df.cache()

# COMMAND ----------

folder_sensor = root_folder+'/incoming_data'
spark_df.drop('damaged').drop('abnormal_sensor').orderBy('timestamp').repartition(100).write.mode('overwrite').format('json').save(folder_sensor)

#Cleanup meta file to only keep names
def cleanup(folder):
  for f in dbutils.fs.ls(folder):
    if not f.name.startswith('part-00'):
      if not f.path.startswith('dbfs:/demos/manufacturing/iot_turbine'):
        raise Exception(f"unexpected path, {f} throwing exception for safety")
      dbutils.fs.rm(f.path)
      
cleanup(folder_sensor)

# COMMAND ----------

from faker import Faker
from pyspark.sql.types import ArrayType, FloatType, StringType
import pyspark.sql.functions as F

Faker.seed(0)
faker = Faker()
fake_latlng = F.udf(lambda: list(faker.local_latlng(country_code = 'US')), ArrayType(StringType()))

# COMMAND ----------

rd = random.Random()
rd.seed(0)
folder = root_folder+'/turbine'
(spark_df.select('turbine_id').drop_duplicates()
   .withColumn('fake_lat_long', fake_latlng())
   .withColumn('model', F.lit('EpicWind'))
   .withColumn('lat', F.col('fake_lat_long').getItem(0))
   .withColumn('long', F.col('fake_lat_long').getItem(1))
   .withColumn('location', F.col('fake_lat_long').getItem(2))
   .withColumn('country', F.col('fake_lat_long').getItem(3))
   .withColumn('state', F.col('fake_lat_long').getItem(4))
   .drop('fake_lat_long')
 .orderBy(F.rand()).repartition(1).write.mode('overwrite').format('json').save(folder))

#Add some turbine with wrong data for expectations
fake_null_uuid = F.udf(lambda: None if rd.randint(0,9) > 2 else str(uuid.uuid4()))
df_error = (spark_df.select('turbine_id').limit(30)
   .withColumn('turbine_id', fake_null_uuid())
   .withColumn('fake_lat_long', fake_latlng())
   .withColumn('model', F.lit('EpicWind'))
   .withColumn('lat', F.lit("ERROR"))
   .withColumn('long', F.lit("ERROR"))
   .withColumn('location', F.col('fake_lat_long').getItem(2))
   .withColumn('country', F.col('fake_lat_long').getItem(3))
   .withColumn('state', F.col('fake_lat_long').getItem(4))
   .drop('fake_lat_long').repartition(1).write.mode('append').format('json').save(folder))
cleanup(folder)

folder_status = root_folder+'/historical_turbine_status'
(spark_df.select('turbine_id', 'abnormal_sensor').drop_duplicates()
         .withColumn('start_time', (F.lit(current_time-1000)-F.rand()*2000).cast('int'))
         .withColumn('end_time', (F.lit(current_time+3600*24*30)+F.rand()*4000).cast('int'))
         .repartition(1).write.mode('overwrite').format('json').save(folder_status))

# COMMAND ----------

# MAGIC %md 
# MAGIC <img src="https://upload.wikimedia.org/wikipedia/commons/5/52/EERE_illust_large_turbine.gif">

# COMMAND ----------

#see https://blog.enerpac.com/wind-turbine-maintenance-components-strategies-and-tools/
#Get the list of states where our wind turbine are
states = spark.read.json(folder).select('state').distinct().collect()
states = [s['state'] for s in states]
#For each state, we'll generate supply chain parts
part_categories = [{'name': 'blade'}, {'name': 'Yaw drive'}, {'name': 'Brake'}, {'name': 'anemometer'}, {'name': 'controller card #1'}, {'name': 'controller card #2'}, {'name': 'Yaw motor'}, {'name': 'hydraulics'}, {'name': 'electronic guidance system'}]
sensors = [c for c in spark.read.json(folder_sensor).columns if "sensor" in c]
parts = []
for p in part_categories:
  for _ in range (1, rd.randint(30, 100)):
    part = {}
    part['EAN'] = None if rd.randint(0,100) > 95 else faker.ean(length=8)
    part['type'] = p['name']
    part['width'] = rd.randint(100,2000)
    part['height'] = rd.randint(100,2000)
    part['weight'] = rd.randint(100,20000)
    part['stock_available'] = rd.randint(0, 5)
    part['stock_location'] =   random.sample(states, 1)[0]
    part['production_time'] = rd.randint(0, 5)
    part['approvisioning_estimated_days'] = rd.randint(30,360)
    part['sensors'] = random.sample(sensors, rd.randint(1,3))
    parts.append(part)
df = spark.createDataFrame(parts)
folder_parts = root_folder+'/parts'
df.repartition(3).write.mode('overwrite').format('json').save(folder_parts)
cleanup(folder_parts)
