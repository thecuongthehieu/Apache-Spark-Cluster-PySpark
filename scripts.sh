conda create -n pyspark python=3.12 pyspark=3.5.3
conda activate pyspark
docker-compose up --build -d --scale spark-worker=2
