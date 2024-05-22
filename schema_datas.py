from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, ArrayType
import pandas as pd

# Create a Spark session
spark = SparkSession.builder \
    .appName("JSON Schema Extraction") \
    .getOrCreate()

# Path to the JSON file
json_path = "/content/newly.json"

# Read the JSON file into a DataFrame
df = spark.read.option("multiline", True).json(json_path)

# Recursive function to flatten the schema
def flatten_schema(schema, prefix=None):
    if len(schema.fields) == 0:
        return []
    def process_field(index, schema, prefix):
        field = schema.fields[index]
        field_name = field.name if prefix is None else f"{prefix}.{field.name}"
        if isinstance(field.dataType, StructType):
            return flatten_schema(field.dataType, field_name)
        elif isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType):
            return flatten_schema(field.dataType.elementType, field_name)
        else:
            return [(field_name, str(field.dataType))]

    fields = process_field(0, schema, prefix)
    remaining_fields = flatten_schema(StructType(schema.fields[1:]), prefix)
    return fields + remaining_fields

# Extract and flatten the schema
flattened_schema = flatten_schema(df.schema)

# Convert the schema to a pandas DataFrame
schema_df = pd.DataFrame(flattened_schema, columns=["Column Name", "Data Type"])

# Path to the CSV file to save the schema
schema_output_path = "/content/resultoutput.csv"

# Write the schema DataFrame to a CSV file
schema_df.to_csv(schema_output_path, index=False)

# Stop the Spark session
spark.stop()
