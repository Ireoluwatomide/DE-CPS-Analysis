# Path: spark_main.py

# Import the required modules
from spark_solution.spark_extract_data import extract_raw_data
from spark_solution.spark_preprocess_data import SparkDataPreprocessor


def main():

    # Extract raw data
    raw_df = extract_raw_data()

    # Preprocess raw data
    preprocessor = SparkDataPreprocessor(raw_df)
    processed_df = preprocessor.preprocess_raw_data()


if __name__ == '__main__':
    main()
