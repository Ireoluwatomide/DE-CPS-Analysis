# Path: spark_solution/spark_preprocess_data.py

# Import the required libraries
from .spark_logger import logs

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import concat, col, expr, to_date, when


class SparkDataPreprocessor:

    def __init__(self, df):

        self.df = df
        self.logger = logs(__name__)

    def preprocess_raw_data(self, df):

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
        df = df.select('ID', 'HRHHIDNEW', 'HRYEARMONTH', *[col for col in df.columns if col not in ['ID', 'HRHHIDNEW', 'HRYEARMONTH']])

        self.logger.info('Preprocessing of Raw Data Completed. Total records preprocessed: {}'.format(df.count()))

        return df

    def replace_negative_values(self, df):
        for col_name in df.columns:
            if col_name != 'HRYEARMONTH':
                df = df.withColumn(col_name, when(df[col_name] == -1, None).otherwise(df[col_name]))

        self.logger.info('Negative values replaced with NULL. Total records preprocessed: {}'.format(df.count()))

        return df

    def rename_columns(self, new_column_names, df):
        for old_col, new_col in zip(df.columns, new_column_names):
            df = df.withColumnRenamed(old_col, new_col)

        self.logger.info('Columns renamed successfully. Total Columns preprocessed: {}'.format(len(df.columns)))

        return df

    def final_processed_data(self):

        new_column_names = ['id', 'household_identifier', 'interview_time', 'final_outcome', 'housing_unit',
                            'household_type', 'telephone_in_house', 'access_to_telephone_elsewhere',
                            'telephone_interview_accepted', 'type_of_interview', 'family_income_range',
                            'geographic_division', 'race']
        df = self.preprocess_raw_data(self.df)
        df = self.replace_negative_values(df)
        processed_df = self.rename_columns(new_column_names, df)

        # processed_df.show(5)

        self.logger.info('Preprocessing completed. Total records preprocessed: {}'.format(processed_df.count()))

        # # Write preprocessed data to .csv file
        # try:
        #     processed_df.write.csv('./out/spark_processed_data', header=True, mode='overwrite')
        #     logger.info('Preprocessed data saved to a CSV file "out/spark_processed_data" successfully.')
        # except Exception as e:
        #     logger.error('Error saving CSV file: {}'.format(str(e)))

        return processed_df
