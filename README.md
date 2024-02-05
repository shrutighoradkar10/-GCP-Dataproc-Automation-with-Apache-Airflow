# -GCP-Dataproc-Automation-with-Apache-Airflow
## Technologies used in the solution:
| Sr.no | Tech               |
|-------|---------------------|
| 1     | GCP Dataproc Cluster|
| 2     | Composer            |
| 3     | Pyspark             |
| 4     | Apache Hive                |
## ARCHITECTURE
![img](https://github.com/shrutighoradkar10/-GCP-Dataproc-Automation-with-Apache-Airflow/assets/75423631/5221c657-515f-4b10-9337-bb0f0f82c991)

## Explanation:
1. Automating a workflow using Apache Airflow to process daily incoming CSV files from a GCP bucket using a Dataproc PySpark job
   and saving the transformed data into a Hive table.
2. Airflow DAG is scheduled to run daily.
3. As soon as the file has arrived in the bucket GCSObjectExistenceSensor operator sense the file and starts processing.
4. In the Pyspark job performing some logical transformations on the data and Writing the transformed data into a Hive table.

![img](https://github.com/shrutighoradkar10/-GCP-Dataproc-Automation-with-Apache-Airflow/assets/75423631/c2a53ce9-3bf4-4b8c-8dab-f28c3a64752f)
