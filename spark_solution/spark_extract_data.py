# Import the required libraries
from .spark_logger import logs
from .spark_session import SparkContextManager

from pyspark.sql.functions import substring
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def extract_raw_data():

    logger = logs(__name__)

    try:
        # Read the input file from the data folder
        file_path = "./data/dec17pub.dat"

        # Initialise the spark session
        spark = SparkContextManager().get_spark_session()

        # Define schema
        schema = StructType([
            StructField('HRHHID', StringType(), False),
            StructField('HRMONTH', IntegerType(), True),
            StructField('HRYEAR4', IntegerType(), True),
            StructField('HUFINAL', IntegerType(), True),
            StructField('HEHOUSUT', StringType(), True),
            StructField('HRHTYPE', IntegerType(), True),
            StructField('HETELHHD', IntegerType(), True),
            StructField('HETELAVL', IntegerType(), True),
            StructField('HEPHONEO', IntegerType(), True),
            StructField('HUINTTYP', IntegerType(), True),
            StructField('HEFAMINC', IntegerType(), True),
            StructField('HRHHID2', StringType(), True),
            StructField('GEDIV', IntegerType(), True),
            StructField('PTDTRACE', IntegerType(), True)
        ])

        # Define the column positions and widths
        column_positions = [(0, 15), (15, 17), (17, 21), (23, 26), (30, 32), (60, 62), (32, 34), (34, 36), (36, 38),
                            (64, 66), (38, 40), (70, 75), (90, 91), (138, 140)]

        # Read the fixed-width data with the specified positions and schema
        raw_df = spark.read.text(file_path) \
            .select(
            *(substring("value", pos[0] + 1, pos[1] - pos[0]).cast(schema.fields[i].dataType).alias(schema.fields[i].name)
                for i, pos in enumerate(column_positions))) \
            .selectExpr("*")

        logger.info("Data extraction completed. Total records extracted: {}".format(raw_df.count()))

        raw_df.show(5)

        # Write extracted data to .csv file
        raw_df.write.csv('./out/spark_raw_data.csv', header=True, mode='overwrite')
        logger.info('Raw Extracted data saved to a CSV file "out/spark_raw_data.csv" successfully.')

        return raw_df

    except Exception as e:
        logger.error(f"Error occurred while extracting raw data: {str(e)}")
