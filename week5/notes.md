### Setup
- [ ] Install Scala and Apache Spark
  - brew install scala
  - brew install apache-spark
- [ ] Configure Conda environment
  - python 3.10
  - pyspark
- [ ] check the path environment variables
  - JAVA_HOME
  - SPARK_HOME
  - SCALA_HOME

### Spark

- use \ a bunch when writing multi-line code, \ before next line
- for sql queries, define how the df can be referenced via .createOrReplaceTempView()
- use action functions (eager execution) to see the results of the transformations (lazy execution)

### gcs
- [ ] create a gcs bucket
- [ ] upload a file to the bucket
- [ ] download the jar dep for spark to use gcs from [here](https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-2.2.5.jar)

### Spark Cluster 
```bash 
# navigate to the spark home directory
cd $SPARK_HOME
```

```bash
# start the master server
./sbin/start-master.sh
```

```bash
# start a worker
./sbin/start-worker.sh <master-spark-URL>
```

```bash
# convert a notebook to a python script for a spark job
jupyter nbconvert --to=script notebook.ipynb
```

```bash
# submit a job to the cluster
spark-submit \
    --master=<master-spark-URL> \
    spark_job.py \
        --input_green=data-dump/taxi/pq/green/2021/*/ \
        --input_yellow=data-dump/taxi/pq/yellow/2021/*/ \
        --output=data/report-2021
```
### Submitting a job to the cluster in cloud
#### Spark to GCS
```bash
# upload the spark job to gcs
gsutil cp spark_job_to_gs.py gs://de-spark-dimi/code/spark_job_to_gs.py
```

```bash
# submit the job to the cluster spark -> gcs
gcloud dataproc jobs submit pyspark \
  --cluster='dt-de-dimi-spark-cluster' \
  --region='europe-west3' \
  'gs://de-spark-dimi/code/spark_job_to_gs.py' \
  -- \
  --input_green='gs://de-spark-dimi/pq/green/2021/*/' \
  --input_yellow='gs://de-spark-dimi/pq/yellow/2021/*/' \
  --output='gs://de-spark-dimi/report-2021'

```
#### Spark to BigQuery
```bash
# upload the spark job to gcs
gsutil cp spark_job_to_bq.py gs://de-spark-dimi/code/spark_job_to_bq.py
```

```bash
# submit the job to the cluster spark -> gcs
gcloud dataproc jobs submit pyspark \
  --cluster='dt-de-dimi-spark-cluster' \
  --region='europe-west3' \
  --jars='gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar' \
  'gs://de-spark-dimi/code/spark_job_to_bq.py' \
  -- \
  --input_green='gs://de-spark-dimi/pq/green/2021/*/' \
  --input_yellow='gs://de-spark-dimi/pq/yellow/2021/*/' \
  --output='spark_reports.report_2021'

```