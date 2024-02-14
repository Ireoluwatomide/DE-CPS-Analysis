# Path: spark_solution/spark_preprocess_data.py

# Import the required libraries
from .spark_logger import logs

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import concat, col, expr, to_date, when


class SparkDataPreprocessor:

    def __init__(self, df):
        self.df = df

    def replace_negative_values(self):
        for col_name in self.df.columns:
            self.df = self.df.withColumn(col_name, when(self.df[col_name] == -1, None).otherwise(self.df[col_name]))
        return self.df

    def preprocess_raw_data(self):

        df = self.replace_negative_values()
        logger = logs(__name__)

        # Concatenate HRHHID and HRHHID2 as HRHHID column
        df = df.withColumn('HRHHIDNEW', concat(col('HRHHID'), col('HRHHID2')))

        # Convert HRYEAR4 and HRMONTH to date and format it to 'YYYY-MMM' format
        df = df.withColumn('HRYEARMONTH', to_date(expr("to_timestamp(concat(HRYEAR4, HRMONTH), 'yyyyMM')")))

        # Delete Redundant Columns and reset index
        df = df.drop('HRHHID', 'HRHHID2', 'HRYEAR4', 'HRMONTH')

        # Remove duplicate records
        df = df.dropDuplicates()

        # Generate an ID column incrementing from 1
        df = df.withColumn('ID', expr('monotonically_increasing_id() + 1').cast(IntegerType()))

        # Reorder columns
        processed_df = df.select('ID', 'HRHHIDNEW', 'HRYEARMONTH', *[col for col in df.columns if col not in ['ID', 'HRHHIDNEW', 'HRYEARMONTH']])

        processed_df.show(5)

        logger.info('Preprocessing completed. Total records preprocessed: {}'.format(processed_df.count()))

        # Write preprocessed data to .csv file
        try:
            processed_df.write.csv('./out/spark_processed_data', header=True, mode='overwrite')
            logger.info('Preprocessed data saved to a CSV file "out/spark_processed_data" successfully.')
        except Exception as e:
            logger.error('Error saving CSV file: {}'.format(str(e)))

        return processed_df
