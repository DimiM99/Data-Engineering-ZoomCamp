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