from pyspark.sql import SparkSession
from google.cloud import storage
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime

bucket_name = "us-central1-airflow-be3c43e3-bucket"
prefix = "Airflow_Ass1/Daily_CSV/"

storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)

client = storage.Client()
blobs = client.list_blobs(bucket_name, prefix=prefix)
most_recent_blob = None
max_created = None
for blob in blobs:
    if max_created is None or blob.time_created > max_created:
        most_recent_blob = blob
        max_created = blob.time_created

file_name = os.path.basename(most_recent_blob.name)


if file_name!="": 
    spark = SparkSession.builder.appName("GCPDataprocJob").enableHiveSupport().getOrCreate()
    file_path = f"gs://us-central1-airflow-be3c43e3-bucket/Airflow_Ass1/Daily_CSV/{file_name}"  #most recent File_Path
    data_Read = spark.read.csv(file_path, header=True, inferSchema=True)
    data_Read.createOrReplaceTempView("health_Data")
        #Top 3 Most Common Diseases
    a=data_Read.groupBy('diagnosis_description').agg(count('*').alias('Diseases_Count')).orderBy(desc('Diseases_Count'))
        
        #Flag for senior patients:
    b=data_Read.withColumn('Flag', when(data_Read["age"] >= 60, 1).otherwise(0))
      #Age Category Disease wise 
    c=spark.sql("""
    SELECT
        diagnosis_description,
        SUM(CASE WHEN age >= 30 AND age <= 40 THEN 1 END) AS `30-40`,
        SUM(CASE WHEN age >= 41 AND age <= 50 THEN 1 ELSE 0 END) AS `41-50`,
        SUM(CASE WHEN age >= 51 AND age <= 60 THEN 1 ELSE 0 END) AS `51-60`,
        SUM(CASE WHEN age >= 61 AND age <= 70 THEN 1 ELSE 0 END) AS `61-70`
    FROM
        health_Data
    GROUP BY
        diagnosis_description
""")
   

    # Replace dots with underscores in file_name
    table_suffix = file_name.replace(".", "_")

    # Write outputs to Hive tables with file_name as suffix
    a.createOrReplaceTempView(f"top3_diseases_{table_suffix}_view")
    spark.sql(f"CREATE TABLE IF NOT EXISTS top3_diseases_{table_suffix}_table AS SELECT * FROM top3_diseases_{table_suffix}_view")

    b.createOrReplaceTempView(f"senior_flag_{table_suffix}_view")
    spark.sql(f"CREATE TABLE IF NOT EXISTS senior_flag_{table_suffix}_table AS SELECT * FROM senior_flag_{table_suffix}_view")

    c.createOrReplaceTempView(f"age_category_{table_suffix}_view")
    spark.sql(f"CREATE TABLE IF NOT EXISTS age_category_{table_suffix}_table AS SELECT * FROM age_category_{table_suffix}_view")

    spark.stop()
   

else:
    print("No files found in the specified directory.")