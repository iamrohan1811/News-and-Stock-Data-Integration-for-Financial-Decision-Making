
import pandas as pd
import mysql.connector
from fuzzywuzzy import fuzz

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

db_config_gav = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Mayank#2019',
    'database': 'gav'  # Database containing global_union_data
}

# Connect to MySQL for iia4
conn_iia4 = mysql.connector.connect(**db_config_iia4)
# Connect to MySQL for iia5
conn_iia5 = mysql.connector.connect(**db_config_iia5)
# Connect to MySQL for gav
conn_gav = mysql.connector.connect(**db_config_gav)

# Load data from MySQL views into Pandas DataFrames
df_nse = pd.read_sql('SELECT * FROM nse_view', conn_iia4)
df_bse = pd.read_sql('SELECT * FROM bse_view', conn_iia5)
df_gav = pd.read_sql('SELECT * FROM global_union_data', conn_gav)

# Define a threshold for fuzzy matching
threshold = 95

# Iterate through rows in the NSE DataFrame and find matching rows in BSE and GAV DataFrames
matches = []
for index_nse, row_nse in df_nse.iterrows():
    for index_bse, row_bse in df_bse.iterrows():
        for index_gav, row_gav in df_gav.iterrows():
            # Check if symbols match exactly or if the names are similar using fuzzy matching
            if (
                row_nse['symbol'] == row_bse['symbol'] == row_gav['symbol'] or
                fuzz.token_sort_ratio(row_nse['name_of_company'], row_bse['name_of_company']) >= threshold or
                fuzz.token_sort_ratio(row_nse['name_of_company'], row_gav['name_of_company']) >= threshold or
                fuzz.token_sort_ratio(row_bse['name_of_company'], row_gav['name_of_company']) >= threshold
            ):
                print("hora hai1")

                matches.append({'nse_index': index_nse, 'bse_index': index_bse, 'gav_index': index_gav})
        print("hora hai")

# Display the matching triples
for match in matches:
    print(f"Match found - NSE Index: {match['nse_index']}, BSE Index: {match['bse_index']}, GAV Index: {match['gav_index']}")

# Close MySQL connections
conn_iia4.close()
conn_iia5.close()
conn_gav.close()

