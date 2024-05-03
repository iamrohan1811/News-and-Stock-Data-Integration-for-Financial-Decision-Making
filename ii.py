import pandas as pd
import mysql.connector
from fuzzywuzzy import fuzz

# MySQL database connection configuration for iia4, iia5, iia6, iia7, and gav databases
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
db_config_iia6= {
    'host': 'localhost',
    'user': 'root',
    'password': 'Mayank#2019',
    'database': 'iia6'  # Database containing bse_view
}

db_config_gav = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Mayank#2019',
    'database': 'gav'  # Database to store gav_11 view
}


# Function to perform entity matching and create the final view
def create_final_view():
    # Load data from MySQL views into Pandas DataFrames using connections
    df_nse = pd.read_sql_query('SELECT * FROM nse_view', con=mysql.connector.connect(**db_config_iia4))
    df_bse = pd.read_sql_query('SELECT * FROM bse_view', con=mysql.connector.connect(**db_config_iia5))
    df_nasdaq = pd.read_sql_query('SELECT * FROM nasdaq_view', con=mysql.connector.connect(**db_config_iia6))

    # Define a threshold for fuzzy matching
    threshold = 80

    # Create an empty DataFrame for the matching results
    matches_df = pd.DataFrame(columns=['symbol', 'name_of_company', 'price'])

    # Iterate through rows in the NSE DataFrame and find matching rows in BSE DataFrame
    for index_nse, row_nse in df_nse.iterrows():
        for index_bse, row_bse in df_bse.iterrows():
            # Check if symbols match exactly or if the names are similar using fuzzy matching
            if row_nse['symbol'] == row_bse['symbol'] or fuzz.token_sort_ratio(row_nse['name_of_company'], row_bse['name_of_company']) >= threshold:
                # Append only relevant columns to matches_df
                matches_df = pd.concat([matches_df, pd.DataFrame([row_nse[['symbol', 'name_of_company', 'price']]])])

    # Iterate through rows in the matches DataFrame and find matching rows in Nasdaq DataFrame
    for index_matches, row_matches in matches_df.iterrows():
        for index_nasdaq, row_nasdaq in df_nasdaq.iterrows():
            # Check if symbols match exactly or if the names are similar using fuzzy matching
            if row_matches['symbol'] == row_nasdaq['symbol'] or fuzz.token_sort_ratio(row_matches['name_of_company'], row_nasdaq['name_of_company']) >= threshold:
                # Append only relevant columns to matches_df
                matches_df = pd.concat([matches_df, pd.DataFrame([row_nasdaq[['symbol', 'name_of_company', 'price']]])])

    # Drop duplicate entries based on the 'symbol' column
    final_view = matches_df.drop_duplicates(subset='symbol')

    # Store the results in the MySQL database
    connection_gav = mysql.connector.connect(**db_config_gav)
    cursor_gav = connection_gav.cursor()

    # Drop the final view if it already exists
    cursor_gav.execute('DROP VIEW IF EXISTS final')

    # Create the final view
    create_view_query = f"""
        CREATE VIEW final AS
        SELECT
            `iia5`.`bse_view`.`symbol` AS `symbol`,
            `iia5`.`bse_view`.`name_of_company` AS `name_of_company`,
            `iia5`.`bse_view`.`price` AS `price`,
            `iia7`.`cal_score`.`cumulative_return` AS `hist_analysis_score`,
            `iia7`.`cal_score`.`score` AS `sentiment_score`
        FROM
            `iia5`.`bse_view`
        JOIN
            `iia7`.`cal_score` ON `iia5`.`bse_view`.`symbol` = `iia7`.`cal_score`.`symbol`

        UNION

        SELECT
            `iia4`.`nse_view`.`symbol` AS `symbol`,
            `iia4`.`nse_view`.`name_of_company` AS `name_of_company`,
            `iia4`.`nse_view`.`price` AS `price`,
            `iia7`.`cal_score`.`cumulative_return` AS `hist_analysis_score`,
            `iia7`.`cal_score`.`score` AS `sentiment_score`
        FROM
            `iia4`.`nse_view`
        JOIN
            `iia7`.`cal_score` ON `iia4`.`nse_view`.`symbol` = `iia7`.`cal_score`.`symbol`

        UNION

        SELECT
            `iia6`.`nasdaq_view`.`symbol` AS `symbol`,
            `iia6`.`nasdaq_view`.`name_of_company` AS `name_of_company`,
            `iia6`.`nasdaq_view`.`price` AS `price`,
            `iia7`.`cal_score`.`cumulative_return` AS `hist_analysis_score`,
            `iia7`.`cal_score`.`score` AS `sentiment_score`
        FROM
            `iia6`.`nasdaq_view`
        JOIN
            `iia7`.`cal_score` ON `iia6`.`nasdaq_view`.`symbol` = `iia7`.`cal_score`.`symbol`;
    """

    cursor_gav.execute(create_view_query)
    connection_gav.commit()
    connection_gav.close()

# Call the function to create the final view
create_final_view()
