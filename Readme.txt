step 1: add these lines after the final dataframe inside the try block:


    schema = df_applicantList_final.schema # df_applicantList_final is dataframe name
    schema_data = [
        {"column_name": field.name, "data_type": str(field.dataType), "nullable": field.nullable}
        for field in schema.fields
    ]
    schema_df = spark.createDataFrame(schema_data).select(
        col("column_name"),
        col("data_type"),
        col("nullable")
    )
    output_path = "/df_applicantList_final"  # Specify the path to the output CSV file
    schema_df.write.option("header", "true").csv(output_path+".csv")

# csv file will created with dataframe name  df_applicantList_final.csv

##########################################################################################################################

step 2: after that u nedd to run the python file


import pandas as pd
import os

# Path to the CSV file
csv_file_path = '/home/dell/Desktop/pyspark_notes/df_applicantList_final.csv'  # Update this to your actual CSV file path
table_name = csv_file_path.split('/')[-1].split('.')[0]

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

# Generate the SQL CREATE TABLE statement
sql_create_table = f'CREATE TABLE {table_name} (\n'
columns = []

for _, row in df.iterrows():
    field_name = row['Field Name']
    data_type = map_data_type(row['Data Type'])
    nullable = '' if row['Nullable'] else ' NOT NULL'
    columns.append(f'    {field_name} {data_type}{nullable}')

sql_create_table += ',\n'.join(columns)
sql_create_table += '\n);'

# Write the SQL query to a file in the current directory
current_directory = os.getcwd()
sql_file_path = os.path.join(current_directory, f'{table_name}.sql')
with open(sql_file_path, 'w') as sql_file:
    sql_file.write(sql_create_table)

print(f"SQL file generated at: {sql_file_path}")

# sql file will be created with ur csv file name df_applicantList_final.sql

##############################################################################################################################
