# Path: python_solution/python_preprocess_data.py

# Import the required libraries
import pandas as pd
from .python_logger import logs


def preprocess_raw_data(df):
    
    logger = logs(__name__)
    
    # Replace -1 with NaN (null)
    df.replace('-1', None, inplace=True)
    
    # Concatenate HRHHID and HRHHID2 as HRHHID column
    df['HRHHIDNEW'] = df['HRHHID'] + df['HRHHID2']
    
    # Convert HRYEAR4 and HRMONTH to datetime object and then format it to 'YYYY-MMM' format
    df['HRYEARMONTH'] = pd.to_datetime(df['HRYEAR4'].astype(str) + df['HRMONTH'], format='%Y%m').dt.strftime('%Y-%m-%d')
    
    # Delete Redundant Columns and reset index
    preprocessed_df = df.drop(['HRHHID', 'HRHHID2', 'HRYEAR4', 'HRMONTH'], axis=1)
    
    # Remove duplicate records  
    preprocessed_df.drop_duplicates(inplace=True)
    
    # Generate an ID column incrementing from 1
    preprocessed_df['ID'] = range(1, len(preprocessed_df) + 1)
    
    # Reorder columns to place 'ID' at index 0, 'HRHHIDNEW' at index 1 and 'HRYEARMONTH' at index 2
    preprocessed_df = preprocessed_df[['ID', 'HRHHIDNEW', 'HRYEARMONTH'] + [col for col in preprocessed_df.columns if col not in ['ID', 'HRHHIDNEW', 'HRYEARMONTH']]]
            
    logger.info('Preprocessing completed. Total records preprocessed: {}'.format(len(preprocessed_df)))
        
    # Write preprocessed data to .csv file
    try:
        preprocessed_df.to_csv('./out/preprocessed_data.csv', index=False)
        logger.info('Preprocessed data saved to a CSV file "out/preprocessed_data.csv" successfully.')
    except Exception as e:
        logger.error('Error saving CSV file: {}'.format(str(e)))

    return preprocessed_df

