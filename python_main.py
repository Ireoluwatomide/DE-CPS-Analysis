# Path: python_main.py

# Import the required libraries
from python_solution.python_db_connect import Database
from python_solution.python_data_model import DataModel
from python_solution.python_load_data import DataLoader
from python_solution.python_extract_data import extract_raw_data
from python_solution.python_preprocess_data import preprocess_raw_data


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
    db = Database()
    db.disconnect()


if __name__ == '__main__':
    main()
