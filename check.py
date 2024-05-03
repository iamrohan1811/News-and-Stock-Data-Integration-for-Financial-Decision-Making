import pandas as pd
import mysql.connector
from fuzzywuzzy import fuzz
from sqlalchemy import create_engine

# MySQL database connection configuration
db_config_iia4 = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Mayank#2019',
    'database': 'iia4'  # Database containing nse_view
}

db_config_iia5 = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Mayank#2019',
    'database': 'iia5'  # Database containing bse_view
}


connection_iia4 = mysql.connector.connect(**db_config_iia4)
connection_iia5 = mysql.connector.connect(**db_config_iia5)

# Load data from MySQL views into Pandas DataFrames using connections
df_nse = pd.read_sql_query('SELECT * FROM nse_view', con=connection_iia4)
df_bse = pd.read_sql_query('SELECT * FROM bse_view', con=connection_iia5)
# Define a threshold for fuzzy matching
threshold = 80

# Iterate through rows in the NSE DataFrame and find matching rows in BSE DataFrame
matches = []
for index_nse, row_nse in df_nse.iterrows():
    for index_bse, row_bse in df_bse.iterrows():
        # Check if symbols match exactly or if the names are similar using fuzzy matching
        if row_nse['symbol'] == row_bse['symbol'] or fuzz.token_sort_ratio(row_nse['name_of_company'], row_bse['name_of_company']) >= threshold:
            matches.append({'nse_index': index_nse, 'bse_index': index_bse})

# Display the matching pairs
for match in matches:
    print(f"Match found - NSE Index: {match['nse_index']}, BSE Index: {match['bse_index']}")

# Close SQLAlchemy connections
connection_iia4.close()
connection_iia5.close()
