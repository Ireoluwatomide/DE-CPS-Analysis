# Path: python_solution/python_extract_data.py

# Import the required libraries
import pandas as pd
from tqdm import tqdm
from .python_logger import logs


def extract_raw_data():
    
    logger = logs(__name__)
    file_path = "./data/dec17pub.dat"

    # Define column names and their positions and sizes based on the data dictionary
    columns = [
        ('HRHHID', 0, 15),
        ('HRMONTH', 15, 2),
        ('HRYEAR4', 17, 4),
        ('HUFINAL', 23, 3),
        ('HEHOUSUT', 30, 2),
        ('HRHTYPE', 60, 2),
        ('HETELHHD', 32, 2),
        ('HETELAVL', 34, 2),
        ('HEPHONEO', 36, 2),
        ('HUINTTYP', 64, 2),
        ('HEFAMINC', 38, 2),
        ('HRHHID2', 70, 5),
        ('GEDIV', 90, 1),
        ('PTDTRACE', 138, 2),
    ]

    # Read data from .dat file and extract data based on the column positions and sizes
    data = []
    try:
        with open(file_path, 'r') as raw_data_file:
            total_lines = sum(1 for _ in raw_data_file)  # Get total number of lines in the file
            raw_data_file.seek(0)  # Reset file pointer to the beginning

            # Iterate over the file using tqdm to show progress bar
            for line in tqdm(raw_data_file, total=total_lines, desc='Extracting data'):
                row = {}
                for col, start, size in columns:
                    row[col] = line[start:start+size].strip()
                data.append(row)

        logger.info('Data extraction completed. Total records extracted: {}'.format(len(data)))
    except FileNotFoundError:
        logger.error('File not found: {}'.format(file_path))

    # Convert extracted data to DataFrame
    raw_df = pd.DataFrame(data)

    # Write extracted data to .csv file
    try:
        raw_df.to_csv('./out/raw_data.csv', index=False)
        logger.info('Raw Extracted data saved to a CSV file "out/raw_data.csv" successfully.')
        logger.info('Total records saved to CSV: {}'.format(len(raw_df)))
    except Exception as e:
        logger.error('Error saving CSV file: {}'.format(str(e)))
        
    return raw_df
