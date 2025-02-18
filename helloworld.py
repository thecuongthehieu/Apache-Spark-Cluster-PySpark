from pyspark.sql import SparkSession
import logging
import sys

def main():
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("HelloWorld") \
        .master("spark://localhost:7077") \
        .getOrCreate()

    # Get Spark logger
    # sc = spark.sparkContext
    # sc.setLogLevel("WARN")
    # logger = sc._jvm.org.apache.log4j.LogManager.getRootLogger()

    def log_data(x):
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger()
        logger.info(f"Processing {x}")
        # print(f"Processing {x}")

    # Create an RDD containing numbers [1, 1000000000)
    numbers_rdd = spark.sparkContext.parallelize(range(1, 1000))

    numbers_rdd.foreach(log_data)

    # Count the elements in the RDD
    count = numbers_rdd.count()

    # logger.error(f"Count of numbers in [1, 1000000000) is: {count}")
    print(f"Count of numbers in [1, 1000000000) is: {count}")

    # Stop the SparkSession
    spark.stop()


if __name__ == "__main__":
    main()
