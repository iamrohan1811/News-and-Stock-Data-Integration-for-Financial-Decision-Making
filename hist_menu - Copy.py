import os
import yfinance as yf
import pandas as pd
import mysql.connector
from datetime import timedelta
import os
import pandas as pd
from datetime import datetime, timedelta  # Add this line
from textblob import TextBlob
import mysql.connector
# MySQL database connection configuration
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Mayank#2019',
    "database": 'IIA7'
}

# Folder containing CSV files
csv_folder = "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\nse\\"
database1 = {
    'iia4': {'table': 'nse_data', 'column': 'NAME_OF_COMPANY'},
    'iia5': {'table': 'bse_data', 'column': 'FinInstrmNm'},
    'iia6': {'table': 'nasdaq', 'column': 'Name'},
}
databases = {
    'iia4': {'table': 'nse_data', 'column': 'SYMBOL'},
    'iia5': {'table': 'bse_data', 'column': 'TckrSymb'},
    'iia6': {'table': 'nasdaq', 'column': 'Symbol'},
    'GAV': {'table': 'global_union_data', 'column': 'symbol'}
}
def perform_historical_analysis(csv_path, company_name):
    df = pd.read_csv(csv_path)
    df['Date'] = pd.to_datetime(df['Date'])

    end_date = df['Date'].max()
    start_date = end_date - timedelta(days=30)
    filtered_data = df[(df['Date'] >= start_date) & (df['Date'] <= end_date)].copy()

    # Use .loc to avoid SettingWithCopyWarning
    filtered_data.loc[:, 'Daily_Return'] = filtered_data['Close'].pct_change()
    filtered_data.loc[:, 'Cumulative_Return'] = (1 + filtered_data['Daily_Return']).cumprod() - 1

    # Check if 'Title' column exists before using it
    sentiment_score = calculate_sentiment(filtered_data['Title'].str.cat(sep=' ')) if 'Title' in filtered_data.columns else 0

    score = filtered_data['Cumulative_Return'].iloc[-1] * 100

    return {'symbol': company_name, 'score': score, 'cumulative_return': filtered_data['Cumulative_Return'].iloc[-1]}

def update_historical_data_in_database(analysis_result):
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # Check if the symbol already exists in the table
    cursor.execute("SELECT * FROM hist_analysis WHERE symbol = %s", (analysis_result['symbol'],))
    existing_data = cursor.fetchone()

    if existing_data:
        # Symbol already exists, update the existing record
        cursor.execute("""
            UPDATE hist_analysis
            SET cumulative_return = %s, score = %s
            WHERE symbol = %s
        """, (analysis_result['cumulative_return'], analysis_result['score'], analysis_result['symbol']))
        print(f"Historical data updated for {analysis_result['symbol']}: Cumulative Return - {analysis_result['cumulative_return']}, Score - {analysis_result['score']}")
    else:
        # Symbol does not exist, insert a new record
        cursor.execute("""
            INSERT INTO hist_analysis (symbol, cumulative_return, score)
            VALUES (%s, %s, %s)
        """, (analysis_result['symbol'], analysis_result['cumulative_return'], analysis_result['score']))
        print(f"Historical data inserted for {analysis_result['symbol']}: Cumulative Return - {analysis_result['cumulative_return']}, Score - {analysis_result['score']}")

    conn.commit()
    conn.close()


# Define a function to update the CSV file for a given company
def update_csv_file(company_symbol, database):
    try:
        if database == 'iia4':
            data = yf.download(f'{company_symbol}.NS')
        elif database == 'iia5':
            data = yf.download(f'{company_symbol}.BO')
        else:
            data = yf.download(company_symbol)

        data.reset_index(inplace=True)
        csv_path = os.path.join(csv_folder, f'{company_symbol}.csv')
        data.to_csv(csv_path, index=False)
        print(f"CSV file for {company_symbol} updated successfully.")
        return csv_path
    except Exception as e:
        print(f"Failed to update CSV file for {company_symbol}: {e}")
        return None

# Define a function to insert data from CSV files into separate MySQL tables
def search_company_data_fromname(company_name, database1):
    results = {}

    for db_name, db_info in database1.items():
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='Mayank#2019',
            database=db_name
        )
        cursor = conn.cursor(dictionary=True)

        query = f"SELECT * FROM {db_info['table']} WHERE {db_info['column']} = %s"

        cursor.execute(query, (company_name,))
        data = cursor.fetchall()

        results[db_name] = data

        conn.close()

    return results


# Function to search data by company name in different databases
def search_company_data(company_name, databases):
    results = {}

    for db_name, db_info in databases.items():
        # Check if the current database is relevant to the task
        if db_name in ['iia4', 'iia5','iia6']:
            conn = mysql.connector.connect(
                host='localhost',
                user='root',
                password='Mayank#2019',
                database=db_name
            )
            cursor = conn.cursor(dictionary=True)

            if 'view' in db_info:
                # Use the view if available
                query = f"SELECT * FROM {db_info['view']} WHERE name_of_company = %s"
            else:
                # Use the table directly
                query = f"SELECT * FROM {db_info['table']} WHERE {db_info['column']} = %s"

            cursor.execute(query, (company_name,))
            data = cursor.fetchall()

            results[db_name] = data

            conn.close()

        elif db_name == 'GAV':
            # Include 'GAV' data in the results without historical analysis or table update
            conn = mysql.connector.connect(
                host='localhost',
                user='root',
                password='Mayank#2019',
                database=db_name
            )
            cursor = conn.cursor(dictionary=True)

            query = f"SELECT * FROM {db_info['table']} WHERE {db_info['column']} = %s"
            cursor.execute(query, (company_name,))
            data = cursor.fetchall()

            results[db_name] = data

            conn.close()

    return results


# Menu-driven program
while True:
    print("\nMenu:")
    print("1. Search company data from symbol")
    print("2. Execute custom query on all databases")
    print("3. Search company data from name")
    print("4. Exit")

    choice = input("Enter your choice (1, 2, 3, or 4): ")
    if choice == '1':
       company_name = input("Enter the company name: ")
       search_results = search_company_data(company_name, databases)
       
       print("\nSearch Results:")
       first_db_found = None  # Initialize first_db_found outside the loop

       for db_name, data in search_results.items():
           print(f"\nData from {databases[db_name]['table']}:")
           if data:
               for row in data:
                   print(row)
               # Get the first non-'GAV' database where the company is found
               if first_db_found is None and db_name != 'GAV':
                   first_db_found = db_name

       if first_db_found:
           print(f"\nPerforming historical analysis and table update")
           csv_path = update_csv_file(company_name, first_db_found)
           if csv_path:
               analysis_results = perform_historical_analysis(csv_path, company_name)
               update_historical_data_in_database(analysis_results)
       else:
           print("No data found.")



    elif choice == '2':
        user_query = input("Enter your custom query: ")
        custom_query_results = execute_custom_query(user_query)

        print("\nCustom Query Results:")
        for db_name, data in custom_query_results.items():
            print(f"\n Requested Query Data :")
            if isinstance(data, list):
                for row in data:
                    print(row)
            else:
                print(data)

    elif choice == '3':
        company_name = input("Enter the company name: ")
        search_results = search_company_data_fromname(company_name, database1)

        print("\nSearch Results:")
        for db_name, data in search_results.items():
            print(f"\nData from {database1[db_name]['table']}:")
            if data:
                for row in data:
                    print(row)
            else:
                print("No data found.")

    elif choice == '4':
        print("Exiting the program. Goodbye!")
        break

    else:
        print("Invalid choice. Please enter 1, 2, 3, or 4.")
