import pandas as pd
import os

# Path to the CSV file
csv_file_path = '/home/dell/Desktop/pyspark_notes/test.csv'  # Update this to your actual CSV file path

# Read the CSV file into a DataFrame
df = pd.read_csv(csv_file_path)

# Create a mapping from the schema data types to SQL data types
type_mapping = {
    'StringType': 'VARCHAR',
    'LongType': 'BIGINT',
    'BooleanType': 'BOOLEAN',
    'IntegerType': 'INT',
    'DoubleType': 'DOUBLE',
    'DateType': 'DATE',
    'TimestampType': 'TIMESTAMP'
    # Add other mappings as needed
}

# Function to map Spark types to SQL types
def map_data_type(data_type):
    # Remove the wrapper (e.g., "StringType()" -> "StringType")
    clean_type = data_type.split('(')[0]
    return type_mapping.get(clean_type, 'VARCHAR')  # Default to VARCHAR if type is not found

# Group by table name and generate the SQL CREATE TABLE and COPY statements
sql_statements = []
grouped = df.groupby('Table Name')

for table_name, group in grouped:
    separator = f'##################\n# {table_name}\n##################'
    sql_statements.append(separator)

    # CREATE TABLE statement
    sql_create_table = f'CREATE TABLE {table_name} (\n'
    columns = []
    copy_columns = []
    for _, row in group.iterrows():
        field_name = row['Field Name']
        data_type = map_data_type(row['Data Type'])
        columns.append(f'    "{field_name}" {data_type}')
        copy_columns.append(f'"{field_name}"')
    sql_create_table += ',\n'.join(columns)
    sql_create_table += '\n);'
    sql_statements.append(sql_create_table)

    # COPY statement
    copy_statement = f'COPY {table_name} (\n\t' + ',\n\t'.join(copy_columns) + '\n);'
    sql_statements.append(copy_statement)

# Write the SQL queries to a file in the current directory
current_directory = os.getcwd()
sql_file_path = os.path.join(current_directory, 'create_and_copy_tables.sql')
with open(sql_file_path, 'w') as sql_file:
    sql_file.write('\n\n'.join(sql_statements))

print(f"SQL file generated at: {sql_file_path}")
