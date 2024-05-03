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
    'database': 'gav'  # Database to store gav_11 view
}

# Function to perform entity matching
def entity_match(df_nse, df_bse, threshold=80):
    matches = []

    for index_nse, row_nse in df_nse.iterrows():
        for index_bse, row_bse in df_bse.iterrows():
            if (
                row_nse['symbol'] == row_bse['symbol'] or
                fuzz.token_sort_ratio(row_nse['name_of_company'], row_bse['name_of_company']) >= threshold
            ):
                matches.append({'nse_index': index_nse, 'bse_index': index_bse})

    return matches

# Load data from MySQL views into Pandas DataFrames using connections
connection_iia4 = mysql.connector.connect(**db_config_iia4)
connection_iia5 = mysql.connector.connect(**db_config_iia5)

df_nse = pd.read_sql_query('SELECT * FROM nse_view', con=connection_iia4)
df_bse = pd.read_sql_query('SELECT * FROM bse_view', con=connection_iia5)

# Perform entity matching
matches = entity_match(df_nse, df_bse)

# Combine matched data into gav_11 DataFrame
gav_11_data = pd.DataFrame()
for match in matches:
    gav_11_data = pd.concat([gav_11_data, df_nse.loc[[match['nse_index']]]])

# Drop duplicates based on 'symbol' column, keeping the row with the lowest 'name_of_company'
gav_11_data = gav_11_data.sort_values(by='name_of_company').drop_duplicates('symbol')

# Store the results in the MySQL database
connection_gav = mysql.connector.connect(**db_config_gav)
cursor_gav = connection_gav.cursor()

# Create a new table 'gav_11_data' and insert data
# Create a new table 'gav_11_data' and insert data
create_table_query = """
    CREATE TABLE IF NOT EXISTS gav_11_data (
        symbol VARCHAR(255),
        name_of_company VARCHAR(255),
        price INT
    )
"""

cursor_gav.execute(create_table_query)

# Insert data into the 'gav_11_data' table
for index, row in gav_11_data.iterrows():
    insert_query = """
        INSERT INTO gav_11_data (symbol, name_of_company, price)
        VALUES (%s, %s, %s)
    """
    # Replace NaN values with None before inserting
    values = (
        row['symbol'],
        row['name_of_company'] if pd.notna(row['name_of_company']) else None,
        row['price'] if pd.notna(row['price']) else None
    )
    cursor_gav.execute(insert_query, values)
# Commit the changes
connection_gav.commit()
# Drop the view if it already exists (optional)
cursor_gav.execute('DROP VIEW IF EXISTS gav_11')

# Create the new view with entity matching data
create_view_query = f"""
    CREATE VIEW gav_11 AS
    SELECT * FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY name_of_company) AS row_num
        FROM gav_11_data
    ) AS ranked
    WHERE row_num = 1
"""

cursor_gav.execute(create_view_query)
connection_gav.commit()

# Close connections
connection_iia4.close()
connection_iia5.close()
connection_gav.close()
