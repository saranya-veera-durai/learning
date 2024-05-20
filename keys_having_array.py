from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructType

# Initialize Spark Session
spark = SparkSession.builder.appName("ComplexNestedJSONExample").getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)

# Load the JSON file
df = spark.read.option("multiline", True).json("/content/newly.json")

# Function to recursively find array fields
def find_array_fields(schema, path='', index=0):
    if index >= len(schema):
        return []  # Base case: If index is out of range, return empty list
    field = schema[index]
    new_path = f"{path}.{field.name}" if path else field.name
    current_fields = []
    
    # Check if current field is an array
    if isinstance(field.dataType, ArrayType):
        current_fields.append(new_path)
        # If ArrayType contains StructType, recursively inspect it
        if isinstance(field.dataType.elementType, StructType):
            current_fields += find_array_fields(field.dataType.elementType.fields, new_path)
    
    # If it's a StructType, recursively check its fields
    if isinstance(field.dataType, StructType):
        current_fields += find_array_fields(field.dataType.fields, new_path)
    
    # Recursively process the next field in the schema
    return current_fields + find_array_fields(schema, path, index + 1)

# Extracting array fields using recursion
array_fields = find_array_fields(df.schema.fields)
print("Nested Array fields found:", array_fields)

Nested Array fields found: ['add', 'educational_qualifications', 'educational_qualifications.subjects', 'educational_qualifications.subjects.score', 'educational_qualifications.subjects.score.res']
