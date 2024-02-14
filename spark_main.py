# Path: spark_main.py

# Import the required modules
from spark_solution.spark_session import SparkContextManager
from spark_solution.spark_warehouse import DataWarehouse
from spark_solution.spark_analysis import SparkQueryExecutor
from spark_solution.spark_extract_data import extract_raw_data
from spark_solution.spark_preprocess_data import SparkDataPreprocessor


def main():

    # Extract raw data
    raw_df = extract_raw_data()

    # Preprocess raw data
    preprocessor = SparkDataPreprocessor(raw_df)
    processed_df = preprocessor.final_processed_data()
    # processed_df.printSchema()

    # Create the Warehouse
    warehouse = DataWarehouse()
    warehouse.create_all_tables()
    warehouse.create_dec_monthly_cps_table(processed_df)

    # Analysis
    query = SparkQueryExecutor()
    query.query_executor()

    # Close the spark session
    SparkContextManager().stop_spark_session()


if __name__ == '__main__':
    main()
