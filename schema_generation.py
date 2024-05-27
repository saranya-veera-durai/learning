from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, ArrayType
import pandas as pd

# Create a Spark session
spark = SparkSession.builder \
    .appName("JSON Schema Extraction") \
    .getOrCreate()

# Path to the JSON file
json_path = "/content/test.json"

# Read the JSON file into a DataFrame
df = spark.read.option("multiline", True).json(json_path)

# Recursive function to flatten the schema
def flatten_schema(schema, prefix=None):
    if len(schema.fields) == 0:
        return []

    def process_field(field, prefix):
        field_name = field.name if prefix is None else f"{prefix}.{field.name}"
        if isinstance(field.dataType, StructType):
            return [(field_name, "struct")] + flatten_schema(field.dataType, field_name)
        elif isinstance(field.dataType, ArrayType):
            element_type = field.dataType.elementType
            if isinstance(element_type, StructType):
                return [(field_name, "array<struct>")] + flatten_schema(element_type, field_name)
            else:
                return [(field_name, f"array<{element_type.simpleString()}>")]
        else:
            return [(field_name, field.dataType.simpleString())]

    fields = []
    for field in schema.fields:
        fields += process_field(field, prefix)
    return fields

# Extract and flatten the schema
flattened_schema = flatten_schema(df.schema)

# Convert the schema to a pandas DataFrame
schema_df = pd.DataFrame(flattened_schema, columns=["Column Name", "Data Type"])

# Group the keys based on their data types
grouped_schema = schema_df.groupby("Data Type")["Column Name"].apply(list).reset_index()

# Path to the CSV file to save the schema
schema_output_path = "/content/test_grouped.csv"

# Write the grouped schema DataFrame to a CSV file
grouped_schema.to_csv(schema_output_path, index=False)

# Stop the Spark session
spark.stop()



