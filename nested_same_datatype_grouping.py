from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, ArrayType
import pandas as pd

# Create a Spark session
spark = SparkSession.builder \
    .appName("JSON Schema Extraction and Categorization") \
    .getOrCreate()

# Path to the JSON file
json_path = "/content/newly.json"

# Read the JSON file into a DataFrame
df = spark.read.option("multiline", True).json(json_path)

# Recursive function to flatten the schema
def flatten_schema(schema, prefix=None):
    fields = []
    for field in schema.fields:
        field_name = field.name if prefix is None else f"{prefix}.{field.name}"
        if isinstance(field.dataType, StructType):
            fields.append((field_name, "struct"))
            fields.extend(flatten_schema(field.dataType, field_name))
        elif isinstance(field.dataType, ArrayType):
            element_type = field.dataType.elementType
            if isinstance(element_type, StructType):
                fields.append((field_name, "array<struct>"))
                fields.extend(flatten_schema(element_type, field_name))
            else:
                fields.append((field_name, f"array<{element_type.simpleString()}>"))
        else:
            fields.append((field_name, field.dataType.simpleString()))
    return fields

# Extract and flatten the schema
flattened_schema = flatten_schema(df.schema)

# Convert to DataFrame
schema_df = pd.DataFrame(flattened_schema, columns=["Column Name", "Data Type"])

# Extract columns with struct type and array<struct> type
struct_columns = schema_df[schema_df["Data Type"] == "struct"]["Column Name"].tolist()
array_struct_columns = schema_df[schema_df["Data Type"] == "array<struct>"]["Column Name"].tolist()

# Function to find immediate parent in dot notation
def find_immediate_parent(column):
    return '.'.join(column.split('.')[:-1])

# Determine parent columns for all columns
all_parents = {col: find_immediate_parent(col) for col in schema_df["Column Name"].tolist()}

# Classify columns
with_struct_parent = [col for col, parent in all_parents.items() if parent in struct_columns and col in array_struct_columns]
without_struct_parent = [col for col in array_struct_columns if all_parents[col] not in struct_columns]
struct_with_array_struct_parent = [col for col in struct_columns if all_parents[col] in array_struct_columns]
struct_without_array_struct_parent = [col for col in struct_columns if all_parents[col] not in array_struct_columns]

# Format lists into single string representations
def format_list(lst):
    return str(lst) if lst else "[]"

with_struct_parent_formatted = format_list(with_struct_parent)
without_struct_parent_formatted = format_list(without_struct_parent)
struct_with_array_struct_parent_formatted = format_list(struct_with_array_struct_parent)
struct_without_array_struct_parent_formatted = format_list(struct_without_array_struct_parent)

# Create a DataFrame for the CSV output
categories = [
    {"Category": "Array<struct> columns with struct parent", "Columns": with_struct_parent_formatted},
    {"Category": "Array<struct> columns without struct parent", "Columns": without_struct_parent_formatted},
    {"Category": "Struct columns with array<struct> parent", "Columns": struct_with_array_struct_parent_formatted},
    {"Category": "Struct columns without array<struct> parent", "Columns": struct_without_array_struct_parent_formatted}
]
categories_df = pd.DataFrame(categories)

# Write the DataFrame to a CSV file
categories_df.to_csv("/content/structured_relationships.csv", index=False)

print("CSV file has been created successfully.")
