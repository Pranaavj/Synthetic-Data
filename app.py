from flask import Flask, request, send_file, render_template
import pandas as pd
import pyodbc
import io
import numpy as np

app = Flask(__name__)

# Database connection string for pyodbc
DATABASE_CONNECTION_STRING = (
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=dw-pia-0004057;'
    'DATABASE=Test;'
    'UID=synthdatauser;'
    'PWD=6pZ!7+ZuWt'
)

def get_db_connection():
    conn = pyodbc.connect(DATABASE_CONNECTION_STRING)
    return conn

def clean_data(df):
    """ Clean and convert data types to match SQL Server expectations. """
    for col in df.columns:
        if pd.api.types.is_float_dtype(df[col]):
            df[col] = pd.to_numeric(df[col], errors='coerce')  # Convert to float, NaN for invalid
        elif pd.api.types.is_integer_dtype(df[col]):
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)  # Convert to int, 0 for NaN
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = pd.to_datetime(df[col], errors='coerce')  # Convert to datetime, NaT for invalid
        elif pd.api.types.is_string_dtype(df[col]):
            df[col].replace('', None, inplace=True)
    return df

def infer_sql_type(pandas_dtype):
    """ Infer SQL data type from pandas data type. """
    if pd.api.types.is_float_dtype(pandas_dtype):
        return 'FLOAT'
    elif pd.api.types.is_integer_dtype(pandas_dtype):
        return 'INT'
    elif pd.api.types.is_datetime64_any_dtype(pandas_dtype):
        return 'DATETIME'
    elif pd.api.types.is_string_dtype(pandas_dtype):
        return 'VARCHAR(MAX)'
    else:
        return 'VARCHAR(MAX)'

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return "No file part", 400

    file = request.files['file']
    
    if file.filename == '':
        return "No selected file", 400

    if file and file.filename.endswith('.csv'):
        try:
            # Read the CSV file into a DataFrame
            df = pd.read_csv(file)

            # Clean and convert data types
            df = clean_data(df)

            # Randomize data in each column independently
            randomized_df = df.apply(lambda x: np.random.permutation(x) if pd.api.types.is_numeric_dtype(x) or pd.api.types.is_string_dtype(x) else x)
            
            # Establish a database connection
            conn = get_db_connection()
            cursor = conn.cursor()

            # Generate a table name based on timestamp or other unique identifier
            table_name = "uploaded_data"

            # Drop table if it exists
            cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE [{table_name}]")
            conn.commit()

            # Create a new table with inferred data types
            columns_with_types = ", ".join([f"[{col}] {infer_sql_type(dtype)}" for col, dtype in zip(df.columns, df.dtypes)])
            create_table_query = f"CREATE TABLE [{table_name}] ({columns_with_types})"
            cursor.execute(create_table_query)
            conn.commit()

            # Insert DataFrame into the table
            for index, row in randomized_df.iterrows():
                values = tuple(row if pd.notna(row) else None for row in row)
                query = f"INSERT INTO [{table_name}] ({', '.join(df.columns)}) VALUES ({', '.join(['?' for _ in df.columns])})"
                try:
                    cursor.execute(query, *values)
                except pyodbc.Error as e:
                    # Log the problematic row and error
                    error_message = f"Error inserting row {index}: {e.args[1]} - Row data: {values}"
                    print(error_message)  # Print to console or use logging
                    return error_message, 500
            conn.commit()

            # Create a CSV output in-memory
            output = io.StringIO()
            randomized_df.to_csv(output, index=False)
            output.seek(0)

            # Clean up the database
            cursor.execute(f"DROP TABLE [{table_name}]")
            conn.commit()

            cursor.close()
            conn.close()

            # Send the randomized CSV as a downloadable file
            return send_file(io.BytesIO(output.getvalue().encode()), download_name='randomized_data.csv', as_attachment=True, mimetype='text/csv')

        except Exception as e:
            return f"Error processing file: {e}", 500

    return "Invalid file type", 400

if __name__ == '__main__':
    app.run(debug=True)
