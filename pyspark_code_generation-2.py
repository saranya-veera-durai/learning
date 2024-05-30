
import pandas as pd
import ast  # Used for safely converting string lists into actual lists

# Load the CSV data
df = pd.read_csv('structured_relationships.csv')

# Strip any unintended whitespace characters from category names
df['Category'] = df['Category'].str.strip()

# Filter columns based on data type
filtered_df = df[df['Category'] == 'Array<struct> columns without struct parent']
print("Filtered DataFrame for Array<struct>:", filtered_df)

if not filtered_df.empty:
    array_columns = ast.literal_eval(filtered_df['Columns'].iloc[0])
else:
    array_columns = []

filtered_df1 = df[df['Category'] == 'Struct columns with array<struct> parent']
print("Filtered DataFrame for Struct columns:", filtered_df1)

if not filtered_df1.empty:
    struct_columns = ast.literal_eval(filtered_df1['Columns'].iloc[0])
else:
    struct_columns = []

print("Array Columns:", array_columns)
print("Struct Columns:", struct_columns)

def write_operations_to_file(input_strings, filename, present):
    import_lines = (
        "from pyspark.sql import SparkSession\n"
        "from pyspark.sql.functions import explode_outer\n\n"
    )

    setup_lines = (
        "spark = SparkSession.builder.appName('DataFrame Operations').getOrCreate()\n"
        "spark.conf.set('spark.sql.repl.eagerEval.enabled', True)\n"
        "df = spark.read.option('multiline', True).json('path/to/your/json/file.json')\n\n"
    )

    footer_lines = "\nspark.stop()\n"

    # Function to process each input string and generate DataFrame operations
    def process_input_string(input_string):
        parts = input_string.split('.')
        result = []
        data = ""

        for i in range(len(parts)):
            if i == 0:
                result.append(parts[i])
            else:
                previous_name_part = "_".join(parts[:i])
                result.append(f"{previous_name_part}.{parts[i]}")

        base_df_name = "df"
        last_df_name = ""  # Keep track of the last DataFrame name
        data += "##############################################################################################################\n"
        data += f"# {'_'.join(parts)}\n"
        data += "##############################################################################################################\n"
        data += "try:\n"

        for i, name in enumerate(result):
            if i == 0:
                df_name = f"df_{name}"
                data += f"    {df_name} = {base_df_name}.select('name', explode_outer('{name}').alias('{name}'))\n"
            else:
                previous_name = result[i-1]
                previous_df_name = f"df_{previous_name.replace('.', '_')}"
                df_name = f"df_{name.replace('.', '_')}"
                data += f"    {df_name} = {previous_df_name}.select('name', explode_outer('{name}').alias('{name.replace('.', '_')}'))\n"
            last_df_name = df_name

        last_column_name = result[-1].replace('.', '_')
        additional_columns = []

        # Check if the last column name is in the 'present' list and if so, add corresponding columns from struct_type
        if last_column_name in present:
            index_matches = [i for i, val in enumerate(present) if val == last_column_name]
            for index in index_matches:
                additional_columns.append(struct_type[index].split('.')[1])

        if additional_columns:
            columns_to_add = "', '".join([f"{last_column_name}.{col+'.*'}" for col in additional_columns])
            data += f"    {last_df_name}_final = {last_df_name}.select('name', '{last_column_name}.*', '{columns_to_add}')\n"
        else:
            data += f"    {last_df_name}_final = {last_df_name}.select('name', '{last_column_name}.*')\n"

        data += f"    {last_df_name}_final.show()\n"

        data += "except Exception as e:\n"
        data += f"    print('Data Not Found in {last_df_name} : ', e)\n\n"

        return data

    input_strings = array_columns

    # Process all input strings and write to file
    with open(filename, 'w') as file:
        file.write(import_lines)
        file.write(setup_lines)
        for input_string in input_strings:
            file.write(process_input_string(input_string))
        file.write(footer_lines)

def replace_dots(struct_columns):
    result = []
    for s in struct_columns:
        parts = s.rsplit('.', 1)
        if len(parts) > 1:
            replaced = parts[0].replace('.', '_') + '.' + parts[1]
        else:
            replaced = parts[0]
        result.append(replaced)
    return result

output_strings = replace_dots(struct_columns)

split_struct_type = [s.split('.') for s in output_strings]

present = [split_entry[0] for split_entry in split_struct_type]

struct_type = output_strings

# Specify the filename for the generated Python script
filename = "/content/new_dataframe_operations.py"

# Generate the Python script file
write_operations_to_file(struct_type, filename, present)
