from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructType

# Initialize Spark Session
spark = SparkSession.builder.appName("ComplexNestedJSONExample").getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)

# Load the JSON file
df = spark.read.option("multiline", True).json("/content/tested.json")


def find_structured_nested_fields(schema, path='', index=0):
    if index >= len(schema):
        return []  # Base case: if index is out of range, return empty list
    
    field = schema[index]
    new_path = f"{path}.{field.name}" if path else field.name
    result_fields = []
    
    # Check if current field is an array or a struct
    if isinstance(field.dataType, ArrayType) or isinstance(field.dataType, StructType):
        result_fields.append(new_path)  # Add the current path to results
        
        # Recurse into elements if it's an ArrayType containing a StructType
        if isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType):
            result_fields += find_structured_nested_fields(field.dataType.elementType.fields, new_path)
        
        # Recurse into fields if it's a StructType
        elif isinstance(field.dataType, StructType):
            result_fields += find_structured_nested_fields(field.dataType.fields, new_path)

    # Recurse for the next field in the schema
    result_fields += find_structured_nested_fields(schema, path, index + 1)
    return result_fields

# Example usage assuming 'df' is your DataFrame loaded with the correct schema
nested_struct_fields = find_structured_nested_fields(df.schema.fields)
print("Structured Nested Fields (Arrays and Structs):", nested_struct_fields)
