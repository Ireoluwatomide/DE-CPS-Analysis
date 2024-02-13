# Path: main.py

# Import the required libraries
from scripts.data_model import DataModel
from scripts.load_data import DataLoader
from scripts.extract_data import extract_raw_data
from scripts.preprocess_data import preprocess_raw_data


def main():
    # Extract raw data
    raw_df = extract_raw_data()

    # Preprocess raw data
    preprocessed_df = preprocess_raw_data(raw_df) 
    
    # Create data model tables
    model = DataModel()
    model.create_schema()
    model.create_all_tables()
    
    # Load data into the tables
    loader = DataLoader()
    loader.load_dec_monthly_cps(preprocessed_df)
    loader.load_all_tables()

    # # Close database connection
    # conn.close()


if __name__ == '__main__':
    main()
