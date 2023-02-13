-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # IOT platform with Databricks Lakehouse - ingesting industrial data for real time analysis
-- MAGIC 
-- MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-full.png " style="float: left; margin-right: 30px" width="600px" />
-- MAGIC 
-- MAGIC <br/>
-- MAGIC 
-- MAGIC ## What is The Databricks Lakehouse for IOT & Manufacturing?
-- MAGIC 
-- MAGIC It's the only enterprise data platform that allows you to leverage all your data, from any source, on any workload to optimize your production lines with real time data, at the lowest cost. 
-- MAGIC 
-- MAGIC The Lakehouse allow you to centralize all your data, from IOT realtime stream to inventory and sales, providing operational speed and efficiency at a scale never before possible.
-- MAGIC 
-- MAGIC 
-- MAGIC ### Simple
-- MAGIC   One single platform and governance/security layer for your data warehousing and AI to **accelerate innovation** and **reduce risks**. No need to stitch together multiple solutions with disparate governance and high complexity.
-- MAGIC 
-- MAGIC ### Open
-- MAGIC   Built on open source and open standards. You own your data and prevent vendor lock-in, with easy integration with external solution. Being open also lets you share your data with any external organization, regardless of their data stack/vendor.
-- MAGIC 
-- MAGIC ### Multicloud
-- MAGIC   One consistent data platform across clouds. Process your data where your need.
-- MAGIC  
-- MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=1444828305810485&aip=1&t=event&ec=dbdemos&ea=VIEW&dp=%2F_dbdemos%2Fmanufacturing%2Flakehouse-iot-platform%2F00-IOT-wind-turbine-introduction-lakehouse&uid=7718718868639865">

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Wind Turbine Predictive Maintenance with the Lakehouse
-- MAGIC 
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/manufacturing/wind_turbine/turbine-photo-open-license.jpg" width="400px" style="float:right; margin-left: 20px"/>
-- MAGIC 
-- MAGIC Being able to collect and centralize industrial equipment information in real time is critical for the industry. Data is the key to unlock critical capabilities such as energy optimization, anomaly detection or predictive maintenance. <br/> 
-- MAGIC 
-- MAGIC Predictive maintenance example include:
-- MAGIC 
-- MAGIC - Predict valve failure in gaz/petrol pipeline to prevent from industrial disaster
-- MAGIC - Detect abnormal behavior in a production line to limit and prevent manufacturing defect in the product
-- MAGIC - Repairing early before larger failure leading to more expensive reparation cost and potential product outage
-- MAGIC 
-- MAGIC ### What we'll build
-- MAGIC 
-- MAGIC In this demo, we'll build a end 2 end IOT platform, collecting data from multiple sources in real time. 
-- MAGIC 
-- MAGIC Based on this information, our analyst have determined that proactively identifing and repairing Wind turbines prior to failure could increase energy production by a major ratio.
-- MAGIC 
-- MAGIC In addition, the business requested a predictive dashboard that would allow their Turbine Maintenance group to monitore the turbines and identify the faulty one. This will also allow us to track our ROI and ensure we reach this extra productivity gain over the year.
-- MAGIC 
-- MAGIC At a very high level, this is the flow we'll implement:
-- MAGIC 
-- MAGIC <img width="1000px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-maintenance.png" />
-- MAGIC 
-- MAGIC 
-- MAGIC 1. Ingest and create our IOT database, with tables easy to query in SQL
-- MAGIC 2. Secure data and grant read access to the Data Analyst and Data Science teams.
-- MAGIC 3. Run BI queries to analyse existing churn
-- MAGIC 4. Build ML model to monitor our wind turbine farm & trigger predictive maintenance operations
-- MAGIC 
-- MAGIC Being able to predict which wind turbine will potentially fail is only the first step to increase our wind turbine farm efficiency. Once we're able to build a model predicting potential maintenance, we can dynamically adapt our spare part stock and even automatically dispatch maintenance team with the proper equipment.
-- MAGIC 
-- MAGIC ### Our dataset
-- MAGIC 
-- MAGIC To simplify this demo, we'll consider that an external system is periodically sending data into our blob storage (S3/ADLS/GCS):
-- MAGIC 
-- MAGIC - Turbine data *(location, model, identifier etc)*
-- MAGIC - Wind turbine sensors, every sec *(energy produced, vibration, typically in streaming)*
-- MAGIC - Turbine status over time, labelled by our analyst team *(historical data to train on model on)*
-- MAGIC 
-- MAGIC *Note that at a technical level, our data could come from any source. Databricks can ingest data from any system (SalesForce, Fivetran, queuing message like kafka, blob storage, SQL & NoSQL databases...).*
-- MAGIC 
-- MAGIC Let's see how this data can be used within the Lakehouse to analyse sensor & trigger predictive maintenance

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 1/ Ingesting and preparing the data (Data Engineering)
-- MAGIC 
-- MAGIC <img style="float: left; margin-right: 20px" width="400px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-1.png" />
-- MAGIC 
-- MAGIC 
-- MAGIC <br/>
-- MAGIC <div style="padding-left: 420px">
-- MAGIC Our first step is to ingest and clean the raw data we received so that our Data Analyst team can start running analysis on top of it.
-- MAGIC 
-- MAGIC 
-- MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo.png" style="float: right; margin-top: 20px" width="200px">
-- MAGIC 
-- MAGIC ### Delta Lake
-- MAGIC 
-- MAGIC All the tables we'll create in the Lakehouse will be stored as Delta Lake table. [Delta Lake](https://delta.io) is an open storage framework for reliability and performance. <br/>
-- MAGIC It provides many functionalities *(ACID Transaction, DELETE/UPDATE/MERGE, Clone zero copy, Change data Capture...)* <br />
-- MAGIC For more details on Delta Lake, run `dbdemos.install('delta-lake')`
-- MAGIC 
-- MAGIC ### Simplify ingestion with Delta Live Tables (DLT)
-- MAGIC 
-- MAGIC Databricks simplifies data ingestion and transformation with Delta Live Tables by allowing SQL users to create advanced pipelines, in batch or streaming. The engine will simplify pipeline deployment and testing and reduce operational complexity, so that you can focus on your business transformation and ensure data quality.<br/>

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Open the Wind Turbine 
-- MAGIC   <a dbdemos-pipeline-id="dlt-iot-wind-turbine" href="#joblist/pipelines/1c10660f-6ad4-4168-9dee-8358d9940763" target="_blank">Delta Live Table pipeline</a> or the [SQL notebook]($./01-Data-ingestion/01.1-DLT-Wind-Turbine-SQL) *(Alternatives: DLT Python version Soon available - [plain Delta+Spark version]($./01-Data-ingestion/plain-spark-delta-pipeline/01.5-Delta-pipeline-spark-iot-turbine))*. <br>
-- MAGIC   For more details on DLT: `dbdemos.install('dlt-load')` or `dbdemos.install('dlt-cdc')`

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 2/ Securing data & governance (Unity Catalog)
-- MAGIC 
-- MAGIC <img style="float: left" width="400px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-5.png" />
-- MAGIC 
-- MAGIC <br/><br/><br/>
-- MAGIC <div style="padding-left: 420px">
-- MAGIC   Now that our first tables have been created, we need to grant our Data Analyst team READ access to be able to start alayzing our Customer churn information.
-- MAGIC   
-- MAGIC   Let's see how Unity Catalog provides Security & governance across our data assets with, including data lineage and audit log.
-- MAGIC   
-- MAGIC   Note that Unity Catalog integrates Delta Sharing, an open protocol to share your data with any external organization, regardless of their stack. For more details:  `dbdemos.install('delta-sharing-airlines')`
-- MAGIC  </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC    Open [Unity Catalog notebook]($./02-Data-governance/02-UC-data-governance-security-iot-turbine) to see how to setup ACL and explore lineage with the Data Explorer.
-- MAGIC   

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 3/ Analysing churn analysis  (BI / Data warehousing / SQL) 
-- MAGIC 
-- MAGIC <img width="300px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-dashboard-1.png"  style="float: right; margin: 100px 0px 10px;"/>
-- MAGIC 
-- MAGIC 
-- MAGIC <img width="400px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-3.png"  style="float: left; margin-right: 10px"/>
-- MAGIC  
-- MAGIC <br><br><br>
-- MAGIC Our datasets are now properly ingested, secured, with a high quality and easily discoverable within our organization.
-- MAGIC 
-- MAGIC Data Analysts are now ready to run BI interactive queries, with low latencies & high througput, including Serverless Datawarehouses providing instant stop & start.
-- MAGIC 
-- MAGIC Let's see how we Data Warehousing can done using Databricks, including with external BI solutions like PowerBI, Tableau and other!

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Open the [Datawarehousing notebook]($./03-BI-data-warehousing/03-BI-Datawarehousing) to start running your BI queries or access or directly open the <a href="/sql/dashboards/e3011ace-8437-40ef-9ba2-5aa7685bccfe" target="_blank">Wind turbine sensor dashboard</a>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 4/ Predict churn with Data Science & Auto-ML
-- MAGIC 
-- MAGIC <img width="400px" style="float: left; margin-right: 10px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-3.png" />
-- MAGIC 
-- MAGIC <br><br><br>
-- MAGIC Being able to run analysis on our past data already gave us a lot of insight to drive our business. We can better understand which customers are churning evaluate the churn impact.
-- MAGIC 
-- MAGIC However, knowing that we have churn isn't enough. We now need to take it to the next level and build a predictive model to determine our customers at risk of churn and increase our revenue.
-- MAGIC 
-- MAGIC This is where the Lakehouse value comes in. Within the same platform, anyone can start building ML model to run such analysis, including low code solution with AutoML.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's see how to train an ML model within 1 click with the [04.1-automl-churn-prediction notebook]($./04-Data-Science-ML/04.1-automl-churn-prediction)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC ## Automate action to reduce Turbine outage based on predictions
-- MAGIC 
-- MAGIC 
-- MAGIC <img style="float: right" width="400px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-dashboard-2.png">
-- MAGIC 
-- MAGIC We now have an end 2 end data pipeline analizing sensor and detecting potential failure before they happeb. We can now easily trigger actions to reduce outages:
-- MAGIC 
-- MAGIC - Schedule maintenance based on teams availability and fault gravity
-- MAGIC - Adjust stocks and part accordingly to predictive maintenance operations
-- MAGIC - Analyze past issues and component failures to improve resilience 
-- MAGIC - and ultimately track our predictive maintenance model efficiency by measuring its efficiency and ROI
-- MAGIC 
-- MAGIC These actions are out of the scope of this demo and simply leverage the Predictive maintenance result from our ML model.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Open the <a href='/sql/dashboards/73c61ddc-cf11-48e1-88ee-ad2f04ee6b9f' target="_blank">DBSQL Predictive maintenance Dashboard</a> to have a complete view of your wind turbine farm, including potential faulty turbines and action to remedy that.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 5/ Deploying and orchestrating the full workflow
-- MAGIC 
-- MAGIC <img style="float: left; margin-right: 10px" width="400px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-4.png" />
-- MAGIC 
-- MAGIC <br><br><br>
-- MAGIC While our data pipeline is almost completed, we're missing one last step: orchestrating the full workflow in production.
-- MAGIC 
-- MAGIC With Databricks Lakehouse, no need to manage an external orchestrator to run your job. Databricks Workflows simplifies all your jobs, with advanced alerting, monitoring, branching options etc.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Open the [workflow and orchestration notebook]($./05-Workflow-orchestration/05-Workflow-orchestration-churn) to schedule our pipeline (data ingetion, model re-training etc)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Conclusion
-- MAGIC 
-- MAGIC We demonstrated how to implement an end 2 end pipeline with the Lakehouse, using a single, unified and secured platform:
-- MAGIC 
-- MAGIC - Data ingestion
-- MAGIC - Data analysis / DW / BI 
-- MAGIC - Data science / ML
-- MAGIC - Workflow & orchestration
-- MAGIC 
-- MAGIC As result, our analyst team was able to simply build a system to not only understand but also forecast future churn and take action accordingly.
-- MAGIC 
-- MAGIC This was only an introduction to the Databricks Platform. For more details, contact your account team and explore more demos with `dbdemos.list()`
