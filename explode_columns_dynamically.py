from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from pyspark.sql.types import StructType, ArrayType, StringType, IntegerType

def flatten_df(schema, df, path=None, index=0):
    if path is None:
        path = []

    # Base case: if index is equal to the number of fields, return the DataFrame
    if index >= len(schema.fields):
        return df

    # Processing each field recursively
    field = schema.fields[index]
    new_path = path + [field.name]
    full_column_name = ".".join(new_path)
    flat_col_name = "_".join(new_path)

    if isinstance(field.dataType, StructType):
        df = flatten_df(field.dataType, df, new_path)
    elif isinstance(field.dataType, ArrayType):
        df = df.withColumn(flat_col_name, explode(col(full_column_name)))
        if isinstance(field.dataType.elementType, StructType):
            df = flatten_df(field.dataType.elementType, df, [flat_col_name])
    else:
        df = df.withColumn(flat_col_name, col(full_column_name))

    # Recursive call for the next field
    df = flatten_df(schema, df, path, index + 1)

    # Optionally drop the parent column if not at the top level
    if path:
        parent_column = "_".join(path[:-1])
        df = df.drop(col(parent_column))

    return df

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Flatten JSON Data") \
    .getOrCreate()
# Enable eager evaluation to view DataFrames automatically
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)

# Path to your JSON file
json_file_path = "/content/newly.json"

# Load JSON data into DataFrame
df = spark.read.option("multiline", True).json(json_file_path)

# Print the schema to understand the data structure
# df.printSchema()

# Flatten the DataFrame
flattened_df = flatten_df(df.schema, df)
flattened_df


+---+--------------------------+----+--------------------------------+--------------------------------------+-----------------------------------+-----------------------------------------+----------------------------------------------+----------------------------------------------+--------------------------------------------+
|age|educational_qualifications|name|educational_qualifications_class|educational_qualifications_school_name|educational_qualifications_subjects|educational_qualifications_subjects_score|educational_qualifications_subjects_score_exam|educational_qualifications_subjects_score_mark|educational_qualifications_subjects_sub_name|
+---+--------------------------+----+--------------------------------+--------------------------------------+-----------------------------------+-----------------------------------------+----------------------------------------------+----------------------------------------------+--------------------------------------------+
| 25|      {10th, Springfiel...| Ram|                            10th|                  Springfield High ...|               {[{mid term, 90}]...|                           {mid term, 90}|                                      mid term|                                            90|                                 Mathematics|
| 25|      {10th, Springfiel...| Ram|                            10th|                  Springfield High ...|               {[{mid term, 70},...|                           {mid term, 70}|                                      mid term|                                            70|                                     Science|
| 25|      {10th, Springfiel...| Ram|                            10th|                  Springfield High ...|               {[{mid term, 70},...|                        {half yearly, 70}|                                   half yearly|                                            70|                                     Science|
| 25|      {10th, Springfiel...| Ram|                            10th|                  Springfield High ...|               {[{mid term, 70},...|                             {annual, 60}|                                        annual|                                            60|                                     Science|
| 25|      {10th, Springfiel...| Ram|                            10th|                  Springfield High ...|               {[{half yearly, 7...|                        {half yearly, 70}|                                   half yearly|                                            70|                                     English|
| 25|      {10th, Springfiel...| Ram|                            10th|                  Springfield High ...|               {[{half yearly, 7...|                             {annual, 80}|                                        annual|                                            80|                                     English|
| 25|      {10th, Springfiel...| Doe|                            10th|                  Springfield High ...|               {[{mid yearly, 70...|                         {mid yearly, 70}|                                    mid yearly|                                            70|                                 Mathematics|
| 25|      {10th, Springfiel...| Doe|                            10th|                  Springfield High ...|               {[{mid yearly, 70...|                             {annual, 80}|                                        annual|                                            80|                                 Mathematics|
| 25|      {10th, Springfiel...| Doe|                            10th|                  Springfield High ...|               {[{mid yearly, 70...|                        {half yearly, 70}|                                   half yearly|                                            70|                                 Mathematics|
| 25|      {10th, Springfiel...| Doe|                            10th|                  Springfield High ...|               {[{half yearly, 7...|                        {half yearly, 70}|                                   half yearly|                                            70|                                     Science|
| 25|      {10th, Springfiel...| Doe|                            10th|                  Springfield High ...|               {[{half yearly, 7...|                        {half yearly, 70}|                                   half yearly|                                            70|                                     Science|
| 25|      {10th, Springfiel...| Doe|                            10th|                  Springfield High ...|               {[{half yearly, 7...|                        {half yearly, 70}|                                   half yearly|                                            70|                                     English|
| 25|      {12th, abc High S...| Doe|                            12th|                       abc High School|               {[{mid term, 70},...|                           {mid term, 70}|                                      mid term|                                            70|                                 Mathematics|
| 25|      {12th, abc High S...| Doe|                            12th|                       abc High School|               {[{mid term, 70},...|                             {annual, 80}|                                        annual|                                            80|                                 Mathematics|
| 25|      {12th, abc High S...| Doe|                            12th|                       abc High School|               {[{mid term, 70},...|                        {half yearly, 70}|                                   half yearly|                                            70|                                 Mathematics|
| 25|      {12th, abc High S...| Doe|                            12th|                       abc High School|               {[{mid term, 70},...|                           {mid term, 70}|                                      mid term|                                            70|                                     Science|
| 25|      {12th, abc High S...| Doe|                            12th|                       abc High School|               {[{mid term, 70},...|                             {annual, 80}|                                        annual|                                            80|                                     Science|
| 25|      {12th, abc High S...| Doe|                            12th|                       abc High School|               {[{mid term, 70},...|                        {half yearly, 70}|                                   half yearly|                                            70|                                     Science|
| 25|      {12th, abc High S...| Doe|                            12th|                       abc High School|               {[{half yearly, 7...|                        {half yearly, 70}|                                   half yearly|                                            70|                                     English|
| 25|      {12th, abc High S...| Doe|                            12th|                       abc High School|               {[{mid term, 70},...|                           {mid term, 70}|                                      mid term|                                            70|                                      social|
+---+--------------------------+----+--------------------------------+--------------------------------------+-----------------------------------+-----------------------------------------+----------------------------------------------+----------------------------------------------+--------------------------------------------+
only showing top 20 rows
