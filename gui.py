import os
import yfinance as yf
import pandas as pd
import mysql.connector
from datetime import timedelta
import tkinter as tk
from tkinter import ttk

# Add the missing function for sentiment analysis
def calculate_sentiment(text):
    blob = TextBlob(text)
    return blob.sentiment.polarity

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

    # Check if the symbol already exists in hist_analysis
    cursor.execute("SELECT * FROM hist_analysis WHERE symbol = %s", (analysis_result['symbol'],))
    existing_data = cursor.fetchone()

    if existing_data:
        # Update the existing record
        cursor.execute("""
            UPDATE hist_analysis
            SET cumulative_return = %s, score = %s
            WHERE symbol = %s
        """, (analysis_result['cumulative_return'], analysis_result['score'], analysis_result['symbol']))
    else:
        # Insert a new record
        cursor.execute("""
            INSERT INTO hist_analysis (symbol, cumulative_return, score)
            VALUES (%s, %s, %s)
        """, (analysis_result['symbol'], analysis_result['cumulative_return'], analysis_result['score']))

    conn.commit()
    conn.close()

    print(f"Historical data updated for {analysis_result['symbol']}: Cumulative Return - {analysis_result['cumulative_return']}, Score - {analysis_result['score']}")

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


def execute_custom_query(query):
    results = {}

    for db_name, db_info in databases.items():
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='Mayank#2019',
            database=db_name
        )
        cursor = conn.cursor(dictionary=True)

        # Replace the database name in the query with the current database
        dynamic_query = query.replace(f'{db_name}.', '')

        try:
            cursor.execute(dynamic_query)
            data = cursor.fetchall()
            results[db_name] = data
        except mysql.connector.Error as err:
            if err.errno == 1146:  # Table doesn't exist error
                results[db_name] = f"Table doesn't exist in {db_name}."
            else:
                raise  # Re-raise the exception if it's not the expected error

        conn.close()

    return results

# Function to create GUI
def create_gui():
    root = tk.Tk()
    root.title("Company Data Analysis")

    # Function to handle the button click
    def handle_button_click():
        user_choice = choice_var.get()

        if user_choice == '1':
            company_name = input_entry.get()
            search_results = search_company_data(company_name, databases)

            # Display the search results
            output_text.delete(1.0, tk.END)  # Clear previous content
            for db_name, data in search_results.items():
                output_text.insert(tk.END, f"\nData from {databases[db_name]['table']}:\n")
                if data:
                    for row in data:
                        output_text.insert(tk.END, row)
                else:
                    output_text.insert(tk.END, "No data found.")

            # Get the first non-'GAV' database where the company is found
            first_db_found = None
            for db_name in databases:
                if first_db_found is None and db_name != 'GAV' and db_name in search_results:
                    first_db_found = db_name

            if first_db_found:
                output_text.insert(tk.END, f"\nPerforming historical analysis and table update\n")
                csv_path = update_csv_file(company_name, first_db_found)
                if csv_path:
                    analysis_results = perform_historical_analysis(csv_path, company_name)
                    update_historical_data_in_database(analysis_results)
                    output_text.insert(tk.END, f"\n{analysis_results}\n")
                else:
                    output_text.insert(tk.END, "Failed to update CSV file.")
            else:
                output_text.insert(tk.END, "No data found.")

        elif user_choice == '2':
            user_query = input("Enter your custom query: ")
            custom_query_results = execute_custom_query(user_query)

            # Display the custom query results
            output_text.delete(1.0, tk.END)  # Clear previous content
            for db_name, data in custom_query_results.items():
                output_text.insert(tk.END, f"\nRequested Query Data from {db_name}:\n")
                if isinstance(data, list):
                    for row in data:
                        output_text.insert(tk.END, row)
                else:
                    output_text.insert(tk.END, data)

        elif user_choice == '3':
            company_name = input("Enter the company name: ")
            search_results = search_company_data_fromname(company_name, database1)

            # Display the search results
            output_text.delete(1.0, tk.END)  # Clear previous content
            for db_name, data in search_results.items():
                output_text.insert(tk.END, f"\nData from {database1[db_name]['table']}:\n")
                if data:
                    for row in data:
                        output_text.insert(tk.END, row)
                else:
                    output_text.insert(tk.END, "No data found.")

        elif user_choice == '4':
            output_text.insert(tk.END, "Exiting the program. Goodbye!")

        else:
            output_text.insert(tk.END, "Invalid choice. Please enter 1, 2, 3, or 4.")

    # Create GUI components
    label = ttk.Label(root, text="Menu:")
    label.grid(row=0, column=0, sticky=tk.W)

    # Create a variable to hold the user's choice
    choice_var = tk.StringVar()
    choice_var.set("1")

    # Radio buttons for menu choices
    choices_frame = ttk.Frame(root)
    choices_frame.grid(row=1, column=0, sticky=tk.W)

    ttk.Radiobutton(choices_frame, text="Search company data from symbol", variable=choice_var, value="1").grid(row=0, column=0, sticky=tk.W)
    ttk.Radiobutton(choices_frame, text="Execute custom query on all databases", variable=choice_var, value="2").grid(row=1, column=0, sticky=tk.W)
    ttk.Radiobutton(choices_frame, text="Search company data from name", variable=choice_var, value="3").grid(row=2, column=0, sticky=tk.W)
    ttk.Radiobutton(choices_frame, text="Exit", variable=choice_var, value="4").grid(row=3, column=0, sticky=tk.W)

    # Entry for company name
    input_entry_label = ttk.Label(root, text="Enter the company name:")
    input_entry_label.grid(row=2, column=0, sticky=tk.W)

    input_entry = ttk.Entry(root)
    input_entry.grid(row=3, column=0, sticky=tk.W)

    # Button to execute user's choice
    execute_button = ttk.Button(root, text="Execute", command=handle_button_click)
    execute_button.grid(row=4, column=0, sticky=tk.W)

    # Text widget for displaying output
    output_text = tk.Text(root, height=15, width=80, wrap=tk.WORD)
    output_text.grid(row=5, column=0, sticky=tk.W, pady=(10, 0))

    root.mainloop()

# Run the GUI
create_gui()
