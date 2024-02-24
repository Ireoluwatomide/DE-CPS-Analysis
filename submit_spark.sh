 spark-submit \
  --master spark://Marvellouss-Air:7077 \
  --deploy-mode client \
  --conf spark.executor.instances=3 \
  --conf spark.executor.memory=2g \
  --conf spark.executor.cores=2 \
  spark_main.py
