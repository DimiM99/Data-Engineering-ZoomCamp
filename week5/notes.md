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