from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col

# Initialize Spark Session
spark = SparkSession.builder.appName("ComplexNestedJSONExample").getOrCreate()

# Make sure to correct "multiline" spelling in the option method
df = spark.read.option("multiline", True).json("/content/newly.json")

# Step 1: Explode the educational_qualifications array
# Make sure to select exploded data properly into a new DataFrame
df1 = df.select(col("*"), explode(col("educational_qualifications")).alias("educational_qualification"))

# Step 2: Flatten the structure
df2 = df1.select("name", "age", "educational_qualification.*")

# Step 3: Explode the subjects array
df3 = df2.select(col("*"), explode(col("subjects")).alias("subject"))

# Step 4: Flatten the structure
df4 = df3.select("name", "age", "class", "school_name", "subject.*")

# Step 5: Explode the score array
df5 = df4.select("*", explode("score").alias("score_detail"))

# Accessing nested fields using the alias from the exploded column
df_final = df5.select(
    "name", "age", "class", "school_name", "sub_name",
    col("score_detail.exam").alias("exam"),
    col("score_detail.mark").alias("mark")
)

# Show the final flattened DataFrame
print("Final Flattened DataFrame:")
df_final.show(truncate=False)

# Stop the Spark session
spark.stop()



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
