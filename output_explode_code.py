from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col

# Initialize Spark Session
spark = SparkSession.builder.appName("ComplexNestedJSONExample").getOrCreate()

# Make sure to correct "multiline" spelling in the option method
df = spark.read.option("multiline", True).json("/content/newly.json")

# Step 1: Explode the educational_qualifications array
# Make sure to select exploded data properly into a new DataFrame
df1 = df.select(col("*"), explode(col("educational_qualifications")).alias("educational_qualification"))
df1.show(truncate=False)
+---+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|age|educational_qualifications                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |name|educational_qualification                                                                                                                                                                                                                                                                           |
+---+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|25 |[{10th, Springfield High School, [{[{mid term, 90}], Mathematics}, {[{mid term, 70}, {half yearly, 70}, {annual, 60}], Science}, {[{half yearly, 70}, {annual, 80}], English}]}]                                                                                                                                                                                                                                                                                                                                                                                                                         |Ram |{10th, Springfield High School, [{[{mid term, 90}], Mathematics}, {[{mid term, 70}, {half yearly, 70}, {annual, 60}], Science}, {[{half yearly, 70}, {annual, 80}], English}]}                                                                                                                      |
|25 |[{10th, Springfield High School, [{[{mid yearly, 70}, {annual, 80}, {half yearly, 70}], Mathematics}, {[{half yearly, 70}, {half yearly, 70}], Science}, {[{half yearly, 70}], English}]}, {12th, abc High School, [{[{mid term, 70}, {annual, 80}, {half yearly, 70}], Mathematics}, {[{mid term, 70}, {annual, 80}, {half yearly, 70}], Science}, {[{half yearly, 70}], English}, {[{mid term, 70}, {half yearly, 70}], social}, {[{mid term, 70}, {annual, 80}, {half yearly, 70}], tamil}]}, {college, sasssc High School, [{[{mid term, 70}], social}, {[{annual, 80}, {half yearly, 70}], tamil}]}]|Doe |{10th, Springfield High School, [{[{mid yearly, 70}, {annual, 80}, {half yearly, 70}], Mathematics}, {[{half yearly, 70}, {half yearly, 70}], Science}, {[{half yearly, 70}], English}]}                                                                                                            |
|25 |[{10th, Springfield High School, [{[{mid yearly, 70}, {annual, 80}, {half yearly, 70}], Mathematics}, {[{half yearly, 70}, {half yearly, 70}], Science}, {[{half yearly, 70}], English}]}, {12th, abc High School, [{[{mid term, 70}, {annual, 80}, {half yearly, 70}], Mathematics}, {[{mid term, 70}, {annual, 80}, {half yearly, 70}], Science}, {[{half yearly, 70}], English}, {[{mid term, 70}, {half yearly, 70}], social}, {[{mid term, 70}, {annual, 80}, {half yearly, 70}], tamil}]}, {college, sasssc High School, [{[{mid term, 70}], social}, {[{annual, 80}, {half yearly, 70}], tamil}]}]|Doe |{12th, abc High School, [{[{mid term, 70}, {annual, 80}, {half yearly, 70}], Mathematics}, {[{mid term, 70}, {annual, 80}, {half yearly, 70}], Science}, {[{half yearly, 70}], English}, {[{mid term, 70}, {half yearly, 70}], social}, {[{mid term, 70}, {annual, 80}, {half yearly, 70}], tamil}]}|
|25 |[{10th, Springfield High School, [{[{mid yearly, 70}, {annual, 80}, {half yearly, 70}], Mathematics}, {[{half yearly, 70}, {half yearly, 70}], Science}, {[{half yearly, 70}], English}]}, {12th, abc High School, [{[{mid term, 70}, {annual, 80}, {half yearly, 70}], Mathematics}, {[{mid term, 70}, {annual, 80}, {half yearly, 70}], Science}, {[{half yearly, 70}], English}, {[{mid term, 70}, {half yearly, 70}], social}, {[{mid term, 70}, {annual, 80}, {half yearly, 70}], tamil}]}, {college, sasssc High School, [{[{mid term, 70}], social}, {[{annual, 80}, {half yearly, 70}], tamil}]}]|Doe |{college, sasssc High School, [{[{mid term, 70}], social}, {[{annual, 80}, {half yearly, 70}], tamil}]}                                                                                                                                                                                             |
|25 |[{10th, Springfield High School, [{[{mid term, 70}, {annual, 80}, {half yearly, 70}], Mathematics}, {[{half yearly, 70}], Science}, {[{annual, 80}, {half yearly, 70}], English}]}, {12th, xyts High School, [{[{mid term, 70}], Mathematics}, {[{mid term, 70}, {annual, 80}, {half yearly, 70}], English}, {[{mid term, 70}, {annual, 80}, {half yearly, 70}], social}, {[{half yearly, 70}], tamil}]}]                                                                                                                                                                                                |John|{10th, Springfield High School, [{[{mid term, 70}, {annual, 80}, {half yearly, 70}], Mathematics}, {[{half yearly, 70}], Science}, {[{annual, 80}, {half yearly, 70}], English}]}                                                                                                                   |
|25 |[{10th, Springfield High School, [{[{mid term, 70}, {annual, 80}, {half yearly, 70}], Mathematics}, {[{half yearly, 70}], Science}, {[{annual, 80}, {half yearly, 70}], English}]}, {12th, xyts High School, [{[{mid term, 70}], Mathematics}, {[{mid term, 70}, {annual, 80}, {half yearly, 70}], English}, {[{mid term, 70}, {annual, 80}, {half yearly, 70}], social}, {[{half yearly, 70}], tamil}]}]                                                                                                                                                                                                |John|{12th, xyts High School, [{[{mid term, 70}], Mathematics}, {[{mid term, 70}, {annual, 80}, {half yearly, 70}], English}, {[{mid term, 70}, {annual, 80}, {half yearly, 70}], social}, {[{half yearly, 70}], tamil}]}                                                                                |
+---+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

# Step 2: Flatten the structure
df2 = df1.select("name", "age", "educational_qualification.*")
df2.show(truncate=False)
+----+---+-------+-----------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|name|age|class  |school_name            |subjects                                                                                                                                                                                                                                                                   |
+----+---+-------+-----------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|Ram |25 |10th   |Springfield High School|[{[{mid term, 90}], Mathematics}, {[{mid term, 70}, {half yearly, 70}, {annual, 60}], Science}, {[{half yearly, 70}, {annual, 80}], English}]                                                                                                                              |
|Doe |25 |10th   |Springfield High School|[{[{mid yearly, 70}, {annual, 80}, {half yearly, 70}], Mathematics}, {[{half yearly, 70}, {half yearly, 70}], Science}, {[{half yearly, 70}], English}]                                                                                                                    |
|Doe |25 |12th   |abc High School        |[{[{mid term, 70}, {annual, 80}, {half yearly, 70}], Mathematics}, {[{mid term, 70}, {annual, 80}, {half yearly, 70}], Science}, {[{half yearly, 70}], English}, {[{mid term, 70}, {half yearly, 70}], social}, {[{mid term, 70}, {annual, 80}, {half yearly, 70}], tamil}]|
|Doe |25 |college|sasssc High School     |[{[{mid term, 70}], social}, {[{annual, 80}, {half yearly, 70}], tamil}]                                                                                                                                                                                                   |
|John|25 |10th   |Springfield High School|[{[{mid term, 70}, {annual, 80}, {half yearly, 70}], Mathematics}, {[{half yearly, 70}], Science}, {[{annual, 80}, {half yearly, 70}], English}]                                                                                                                           |
|John|25 |12th   |xyts High School       |[{[{mid term, 70}], Mathematics}, {[{mid term, 70}, {annual, 80}, {half yearly, 70}], English}, {[{mid term, 70}, {annual, 80}, {half yearly, 70}], social}, {[{half yearly, 70}], tamil}]                                                                                 |
+----+---+-------+-----------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

# Step 3: Explode the subjects array
df3 = df2.select(col("*"), explode(col("subjects")).alias("subject"))
df3.show(truncate=False)
+----+---+-------+-----------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------+
|name|age|class  |school_name            |subjects                                                                                                                                                                                                                                                                   |subject                                                           |
+----+---+-------+-----------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------+
|Ram |25 |10th   |Springfield High School|[{[{mid term, 90}], Mathematics}, {[{mid term, 70}, {half yearly, 70}, {annual, 60}], Science}, {[{half yearly, 70}, {annual, 80}], English}]                                                                                                                              |{[{mid term, 90}], Mathematics}                                   |
|Ram |25 |10th   |Springfield High School|[{[{mid term, 90}], Mathematics}, {[{mid term, 70}, {half yearly, 70}, {annual, 60}], Science}, {[{half yearly, 70}, {annual, 80}], English}]                                                                                                                              |{[{mid term, 70}, {half yearly, 70}, {annual, 60}], Science}      |
|Ram |25 |10th   |Springfield High School|[{[{mid term, 90}], Mathematics}, {[{mid term, 70}, {half yearly, 70}, {annual, 60}], Science}, {[{half yearly, 70}, {annual, 80}], English}]                                                                                                                              |{[{half yearly, 70}, {annual, 80}], English}                      |
|Doe |25 |10th   |Springfield High School|[{[{mid yearly, 70}, {annual, 80}, {half yearly, 70}], Mathematics}, {[{half yearly, 70}, {half yearly, 70}], Science}, {[{half yearly, 70}], English}]                                                                                                                    |{[{mid yearly, 70}, {annual, 80}, {half yearly, 70}], Mathematics}|
|Doe |25 |10th   |Springfield High School|[{[{mid yearly, 70}, {annual, 80}, {half yearly, 70}], Mathematics}, {[{half yearly, 70}, {half yearly, 70}], Science}, {[{half yearly, 70}], English}]                                                                                                                    |{[{half yearly, 70}, {half yearly, 70}], Science}                 |
|Doe |25 |10th   |Springfield High School|[{[{mid yearly, 70}, {annual, 80}, {half yearly, 70}], Mathematics}, {[{half yearly, 70}, {half yearly, 70}], Science}, {[{half yearly, 70}], English}]                                                                                                                    |{[{half yearly, 70}], English}                                    |
|Doe |25 |12th   |abc High School        |[{[{mid term, 70}, {annual, 80}, {half yearly, 70}], Mathematics}, {[{mid term, 70}, {annual, 80}, {half yearly, 70}], Science}, {[{half yearly, 70}], English}, {[{mid term, 70}, {half yearly, 70}], social}, {[{mid term, 70}, {annual, 80}, {half yearly, 70}], tamil}]|{[{mid term, 70}, {annual, 80}, {half yearly, 70}], Mathematics}  |
|Doe |25 |12th   |abc High School        |[{[{mid term, 70}, {annual, 80}, {half yearly, 70}], Mathematics}, {[{mid term, 70}, {annual, 80}, {half yearly, 70}], Science}, {[{half yearly, 70}], English}, {[{mid term, 70}, {half yearly, 70}], social}, {[{mid term, 70}, {annual, 80}, {half yearly, 70}], tamil}]|{[{mid term, 70}, {annual, 80}, {half yearly, 70}], Science}      |
|Doe |25 |12th   |abc High School        |[{[{mid term, 70}, {annual, 80}, {half yearly, 70}], Mathematics}, {[{mid term, 70}, {annual, 80}, {half yearly, 70}], Science}, {[{half yearly, 70}], English}, {[{mid term, 70}, {half yearly, 70}], social}, {[{mid term, 70}, {annual, 80}, {half yearly, 70}], tamil}]|{[{half yearly, 70}], English}                                    |
|Doe |25 |12th   |abc High School        |[{[{mid term, 70}, {annual, 80}, {half yearly, 70}], Mathematics}, {[{mid term, 70}, {annual, 80}, {half yearly, 70}], Science}, {[{half yearly, 70}], English}, {[{mid term, 70}, {half yearly, 70}], social}, {[{mid term, 70}, {annual, 80}, {half yearly, 70}], tamil}]|{[{mid term, 70}, {half yearly, 70}], social}                     |
|Doe |25 |12th   |abc High School        |[{[{mid term, 70}, {annual, 80}, {half yearly, 70}], Mathematics}, {[{mid term, 70}, {annual, 80}, {half yearly, 70}], Science}, {[{half yearly, 70}], English}, {[{mid term, 70}, {half yearly, 70}], social}, {[{mid term, 70}, {annual, 80}, {half yearly, 70}], tamil}]|{[{mid term, 70}, {annual, 80}, {half yearly, 70}], tamil}        |
|Doe |25 |college|sasssc High School     |[{[{mid term, 70}], social}, {[{annual, 80}, {half yearly, 70}], tamil}]                                                                                                                                                                                                   |{[{mid term, 70}], social}                                        |
|Doe |25 |college|sasssc High School     |[{[{mid term, 70}], social}, {[{annual, 80}, {half yearly, 70}], tamil}]                                                                                                                                                                                                   |{[{annual, 80}, {half yearly, 70}], tamil}                        |
|John|25 |10th   |Springfield High School|[{[{mid term, 70}, {annual, 80}, {half yearly, 70}], Mathematics}, {[{half yearly, 70}], Science}, {[{annual, 80}, {half yearly, 70}], English}]                                                                                                                           |{[{mid term, 70}, {annual, 80}, {half yearly, 70}], Mathematics}  |
|John|25 |10th   |Springfield High School|[{[{mid term, 70}, {annual, 80}, {half yearly, 70}], Mathematics}, {[{half yearly, 70}], Science}, {[{annual, 80}, {half yearly, 70}], English}]                                                                                                                           |{[{half yearly, 70}], Science}                                    |
|John|25 |10th   |Springfield High School|[{[{mid term, 70}, {annual, 80}, {half yearly, 70}], Mathematics}, {[{half yearly, 70}], Science}, {[{annual, 80}, {half yearly, 70}], English}]                                                                                                                           |{[{annual, 80}, {half yearly, 70}], English}                      |
|John|25 |12th   |xyts High School       |[{[{mid term, 70}], Mathematics}, {[{mid term, 70}, {annual, 80}, {half yearly, 70}], English}, {[{mid term, 70}, {annual, 80}, {half yearly, 70}], social}, {[{half yearly, 70}], tamil}]                                                                                 |{[{mid term, 70}], Mathematics}                                   |
|John|25 |12th   |xyts High School       |[{[{mid term, 70}], Mathematics}, {[{mid term, 70}, {annual, 80}, {half yearly, 70}], English}, {[{mid term, 70}, {annual, 80}, {half yearly, 70}], social}, {[{half yearly, 70}], tamil}]                                                                                 |{[{mid term, 70}, {annual, 80}, {half yearly, 70}], English}      |
|John|25 |12th   |xyts High School       |[{[{mid term, 70}], Mathematics}, {[{mid term, 70}, {annual, 80}, {half yearly, 70}], English}, {[{mid term, 70}, {annual, 80}, {half yearly, 70}], social}, {[{half yearly, 70}], tamil}]                                                                                 |{[{mid term, 70}, {annual, 80}, {half yearly, 70}], social}       |
|John|25 |12th   |xyts High School       |[{[{mid term, 70}], Mathematics}, {[{mid term, 70}, {annual, 80}, {half yearly, 70}], English}, {[{mid term, 70}, {annual, 80}, {half yearly, 70}], social}, {[{half yearly, 70}], tamil}]                                                                                 |{[{half yearly, 70}], tamil}                                      |
+----+---+-------+-----------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------+

# Step 4: Flatten the structure
df4 = df3.select("name", "age", "class", "school_name", "subject.*")
df4.show(truncate=False)
+----+---+-------+-----------------------+---------------------------------------------------+-----------+
|name|age|class  |school_name            |score                                              |sub_name   |
+----+---+-------+-----------------------+---------------------------------------------------+-----------+
|Ram |25 |10th   |Springfield High School|[{mid term, 90}]                                   |Mathematics|
|Ram |25 |10th   |Springfield High School|[{mid term, 70}, {half yearly, 70}, {annual, 60}]  |Science    |
|Ram |25 |10th   |Springfield High School|[{half yearly, 70}, {annual, 80}]                  |English    |
|Doe |25 |10th   |Springfield High School|[{mid yearly, 70}, {annual, 80}, {half yearly, 70}]|Mathematics|
|Doe |25 |10th   |Springfield High School|[{half yearly, 70}, {half yearly, 70}]             |Science    |
|Doe |25 |10th   |Springfield High School|[{half yearly, 70}]                                |English    |
|Doe |25 |12th   |abc High School        |[{mid term, 70}, {annual, 80}, {half yearly, 70}]  |Mathematics|
|Doe |25 |12th   |abc High School        |[{mid term, 70}, {annual, 80}, {half yearly, 70}]  |Science    |
|Doe |25 |12th   |abc High School        |[{half yearly, 70}]                                |English    |
|Doe |25 |12th   |abc High School        |[{mid term, 70}, {half yearly, 70}]                |social     |
|Doe |25 |12th   |abc High School        |[{mid term, 70}, {annual, 80}, {half yearly, 70}]  |tamil      |
|Doe |25 |college|sasssc High School     |[{mid term, 70}]                                   |social     |
|Doe |25 |college|sasssc High School     |[{annual, 80}, {half yearly, 70}]                  |tamil      |
|John|25 |10th   |Springfield High School|[{mid term, 70}, {annual, 80}, {half yearly, 70}]  |Mathematics|
|John|25 |10th   |Springfield High School|[{half yearly, 70}]                                |Science    |
|John|25 |10th   |Springfield High School|[{annual, 80}, {half yearly, 70}]                  |English    |
|John|25 |12th   |xyts High School       |[{mid term, 70}]                                   |Mathematics|
|John|25 |12th   |xyts High School       |[{mid term, 70}, {annual, 80}, {half yearly, 70}]  |English    |
|John|25 |12th   |xyts High School       |[{mid term, 70}, {annual, 80}, {half yearly, 70}]  |social     |
|John|25 |12th   |xyts High School       |[{half yearly, 70}]                                |tamil      |
+----+---+-------+-----------------------+---------------------------------------------------+-----------+
# Step 5: Explode the score array
df5 = df4.select("*", explode("score").alias("score_detail"))
df5.show(truncate=False)
+----+---+-----+-----------------------+---------------------------------------------------+-----------+-----------------+
|name|age|class|school_name            |score                                              |sub_name   |score_detail     |
+----+---+-----+-----------------------+---------------------------------------------------+-----------+-----------------+
|Ram |25 |10th |Springfield High School|[{mid term, 90}]                                   |Mathematics|{mid term, 90}   |
|Ram |25 |10th |Springfield High School|[{mid term, 70}, {half yearly, 70}, {annual, 60}]  |Science    |{mid term, 70}   |
|Ram |25 |10th |Springfield High School|[{mid term, 70}, {half yearly, 70}, {annual, 60}]  |Science    |{half yearly, 70}|
|Ram |25 |10th |Springfield High School|[{mid term, 70}, {half yearly, 70}, {annual, 60}]  |Science    |{annual, 60}     |
|Ram |25 |10th |Springfield High School|[{half yearly, 70}, {annual, 80}]                  |English    |{half yearly, 70}|
|Ram |25 |10th |Springfield High School|[{half yearly, 70}, {annual, 80}]                  |English    |{annual, 80}     |
|Doe |25 |10th |Springfield High School|[{mid yearly, 70}, {annual, 80}, {half yearly, 70}]|Mathematics|{mid yearly, 70} |
|Doe |25 |10th |Springfield High School|[{mid yearly, 70}, {annual, 80}, {half yearly, 70}]|Mathematics|{annual, 80}     |
|Doe |25 |10th |Springfield High School|[{mid yearly, 70}, {annual, 80}, {half yearly, 70}]|Mathematics|{half yearly, 70}|
|Doe |25 |10th |Springfield High School|[{half yearly, 70}, {half yearly, 70}]             |Science    |{half yearly, 70}|
|Doe |25 |10th |Springfield High School|[{half yearly, 70}, {half yearly, 70}]             |Science    |{half yearly, 70}|
|Doe |25 |10th |Springfield High School|[{half yearly, 70}]                                |English    |{half yearly, 70}|
|Doe |25 |12th |abc High School        |[{mid term, 70}, {annual, 80}, {half yearly, 70}]  |Mathematics|{mid term, 70}   |
|Doe |25 |12th |abc High School        |[{mid term, 70}, {annual, 80}, {half yearly, 70}]  |Mathematics|{annual, 80}     |
|Doe |25 |12th |abc High School        |[{mid term, 70}, {annual, 80}, {half yearly, 70}]  |Mathematics|{half yearly, 70}|
|Doe |25 |12th |abc High School        |[{mid term, 70}, {annual, 80}, {half yearly, 70}]  |Science    |{mid term, 70}   |
|Doe |25 |12th |abc High School        |[{mid term, 70}, {annual, 80}, {half yearly, 70}]  |Science    |{annual, 80}     |
|Doe |25 |12th |abc High School        |[{mid term, 70}, {annual, 80}, {half yearly, 70}]  |Science    |{half yearly, 70}|
|Doe |25 |12th |abc High School        |[{half yearly, 70}]                                |English    |{half yearly, 70}|
|Doe |25 |12th |abc High School        |[{mid term, 70}, {half yearly, 70}]                |social     |{mid term, 70}   |
+----+---+-----+-----------------------+---------------------------------------------------+-----------+-----------------+
only showing top 20 rows

# Accessing nested fields using the alias from the exploded column
df_final = df5.select(
    "name", "age", "class", "school_name", "sub_name",
    col("score_detail.exam").alias("exam"),
    col("score_detail.mark").alias("mark")
)

# Show the final flattened DataFrame
print("Final Flattened DataFrame:")
df_final.show(truncate=False)
Final Flattened DataFrame:
+----+---+-----+-----------------------+-----------+-----------+----+
|name|age|class|school_name            |sub_name   |exam       |mark|
+----+---+-----+-----------------------+-----------+-----------+----+
|Ram |25 |10th |Springfield High School|Mathematics|mid term   |90  |
|Ram |25 |10th |Springfield High School|Science    |mid term   |70  |
|Ram |25 |10th |Springfield High School|Science    |half yearly|70  |
|Ram |25 |10th |Springfield High School|Science    |annual     |60  |
|Ram |25 |10th |Springfield High School|English    |half yearly|70  |
|Ram |25 |10th |Springfield High School|English    |annual     |80  |
|Doe |25 |10th |Springfield High School|Mathematics|mid yearly |70  |
|Doe |25 |10th |Springfield High School|Mathematics|annual     |80  |
|Doe |25 |10th |Springfield High School|Mathematics|half yearly|70  |
|Doe |25 |10th |Springfield High School|Science    |half yearly|70  |
|Doe |25 |10th |Springfield High School|Science    |half yearly|70  |
|Doe |25 |10th |Springfield High School|English    |half yearly|70  |
|Doe |25 |12th |abc High School        |Mathematics|mid term   |70  |
|Doe |25 |12th |abc High School        |Mathematics|annual     |80  |
|Doe |25 |12th |abc High School        |Mathematics|half yearly|70  |
|Doe |25 |12th |abc High School        |Science    |mid term   |70  |
|Doe |25 |12th |abc High School        |Science    |annual     |80  |
|Doe |25 |12th |abc High School        |Science    |half yearly|70  |
|Doe |25 |12th |abc High School        |English    |half yearly|70  |
|Doe |25 |12th |abc High School        |social     |mid term   |70  |
+----+---+-----+-----------------------+-----------+-----------+----+
only showing top 20 rows

# Stop the Spark session
spark.stop()

