# !pip install pyspark

# ########################################## test.json curlybrace type #######################################################
from google.colab import files
uploaded = files.upload()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size, expr

# Create a Spark session
spark = SparkSession.builder.appName('EducationalQualificationsCount').getOrCreate()

# Load the JSON file into a DataFrame
path_to_json = "/content/test.json"
df = spark.read.option("multiline", "true").json(path_to_json)

# Convert the struct to a JSON string, then parse it back as a map to count the keys
df_with_qualifications_count = df.select(
    col("name"),
    col("age"),
    size(expr("map_keys(from_json(to_json(educational_qualifications), 'map<string,string>'))")).alias("qualifications_count")
)

# Show the DataFrame
df_with_qualifications_count.show()

# Stop the Spark session if necessary
spark.stop()

+----+---+--------------------+
|name|age|qualifications_count|
+----+---+--------------------+
| Ram| 25|                   1|
| Doe| 25|                   3|
|John| 25|                   2|
+----+---+--------------------+

# --------------------------------------------------------------------


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size

# Create a Spark session
spark = SparkSession.builder.appName('SubjectCount').getOrCreate()

# Load the JSON file into a DataFrame
path_to_json = "/content/test.json"
df = spark.read.option("multiline", "true").json(path_to_json)

# Select and count the subjects for each educational level
df_subject_count = df.select(
    col("name"),
    col("age"),
    size(col("educational_qualifications.10th_grade.subjects")).alias("10th_grade_subject_count"),
    size(col("educational_qualifications.12th_grade.subjects")).alias("12th_grade_subject_count"),
    size(col("educational_qualifications.college.subjects")).alias("college_subject_count")
)

# Show the DataFrame
df_subject_count.show()


+----+---+------------------------+------------------------+---------------------+
|name|age|10th_grade_subject_count|12th_grade_subject_count|college_subject_count|
+----+---+------------------------+------------------------+---------------------+
| Ram| 25|                       3|                      -1|                   -1|
| Doe| 25|                       3|                       2|                    1|
|John| 25|                       3|                       3|                   -1|
+----+---+------------------------+------------------------+---------------------+



# ############################################### testing.json array type#####################################################


from google.colab import files
uploaded = files.upload()


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size

# Create a Spark session
spark = SparkSession.builder.appName('SubjectCount').getOrCreate()

# Load the JSON file into a DataFrame
path_to_json = "/content/testing.json"
df = spark.read.option("multiline", "true").json(path_to_json)

# Select and count the subjects for each educational level
df_subject_count = df.select(
    col("name"),
    col("age"),
    size(col("educational_qualifications")).alias("class_count")
)

# Show the DataFrame
df_subject_count.show()

+----+---+-----------+
|name|age|class_count|
+----+---+-----------+
| Ram| 25|          1|
| Doe| 25|          3|
|John| 25|          2|
+----+---+-----------+


# ------------------------------------------------------------------------



from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, size

# Create a Spark session
spark = SparkSession.builder.appName('SubjectCount').getOrCreate()

# Load the JSON file into a DataFrame
path_to_json = "/content/testing.json"
df = spark.read.option("multiline", "true").json(path_to_json)

# Explode the educational_qualifications array to separate rows
df_exploded = df.select(
    col("name"),
    col("age"),
    explode(col("educational_qualifications")).alias("qualification")
)

# Now, select the relevant fields and count the number of subjects
df_subject_count = df_exploded.select(
    col("name"),
    col("age"),
    col("qualification.class").alias("class_level"),
    size(col("qualification.subjects")).alias("subject_count")
)

# Show the DataFrame
df_subject_count.show()


+----+---+-----------+-------------+
|name|age|class_level|subject_count|
+----+---+-----------+-------------+
| Ram| 25|       10th|            3|
| Doe| 25|       10th|            3|
| Doe| 25|       12th|            5|
| Doe| 25|    college|            2|
|John| 25|       10th|            3|
|John| 25|       12th|            4|
+----+---+-----------+-------------+


# ------------------------------------------------------------------------


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, size, count
from pyspark.sql.window import Window

# Create a Spark session
spark = SparkSession.builder.appName('SubjectCount').getOrCreate()

# Load the JSON file into a DataFrame
path_to_json = "/content/testing.json"
df = spark.read.option("multiline", "true").json(path_to_json)

# Explode the educational_qualifications array to separate rows
df_exploded = df.select(
    col("name"),
    col("age"),
    explode(col("educational_qualifications")).alias("qualification")
)

# Now, select the relevant fields and count the number of subjects
df_subject_count = df_exploded.select(
    col("name"),
    col("age"),
    col("qualification.class").alias("class_level"),
    size(col("qualification.subjects")).alias("subject_count")
)

# Define a window specification which partitions data by name
windowSpec = Window.partitionBy("name")

# Add a column to count qualifications for each person using the window specification
df_with_qual_count = df_subject_count.withColumn(
    "qualifications_count",
    count(col("class_level")).over(windowSpec)
)

# Show the DataFrame
df_with_qual_count.show()


+----+---+-----------+-------------+--------------------+
|name|age|class_level|subject_count|qualifications_count|
+----+---+-----------+-------------+--------------------+
| Doe| 25|       10th|            3|                   3|
| Doe| 25|       12th|            5|                   3|
| Doe| 25|    college|            2|                   3|
|John| 25|       10th|            3|                   2|
|John| 25|       12th|            4|                   2|
| Ram| 25|       10th|            3|                   1|
+----+---+-----------+-------------+--------------------+

# ############################################### new.json nested array type#####################################################

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, size, count
from pyspark.sql.window import Window

# Create a Spark session
spark = SparkSession.builder.appName('ScoreCount').getOrCreate()

# Load the JSON file into a DataFrame
path_to_json = "/content/new.json"
df = spark.read.option("multiline", "true").json(path_to_json)

# Explode the educational_qualifications array to separate rows
df_exploded = df.select(
    col("name"),
    col("age"),
    explode(col("educational_qualifications")).alias("qualification")
)

# Further explode the subjects array
df_exploded_subjects = df_exploded.select(
    col("name"),
    col("age"),
    col("qualification.class").alias("class_level"),
    explode(col("qualification.subjects")).alias("subject")
)

# Explode the score array within each subject
df_exploded_scores = df_exploded_subjects.select(
    col("name"),
    col("age"),
    col("class_level"),
    col("subject.name").alias("subject_name"),
    explode(col("subject.score")).alias("score_detail")
)

# Now, count the scores for each subject
df_score_count = df_exploded_scores.groupBy("name", "age", "class_level", "subject_name").agg(
    count("score_detail").alias("score_count")
)

# Define a window specification which partitions data by name
windowSpec = Window.partitionBy("name")

# Add a column to count qualifications for each person using the window specification
df_with_qual_count = df_score_count.withColumn(
    "qualifications_count",
    count(col("class_level")).over(windowSpec)
)

# Show the DataFrame
df_with_qual_count.show()

# Stop the Spark session if necessary
spark.stop()

+----+---+-----------+------------+-----------+--------------------+
|name|age|class_level|subject_name|score_count|qualifications_count|
+----+---+-----------+------------+-----------+--------------------+
| Doe| 25|       12th|       tamil|          3|                  10|
| Doe| 25|       10th|     Science|          2|                  10|
| Doe| 25|       12th|      social|          2|                  10|
| Doe| 25|       12th|     Science|          3|                  10|
| Doe| 25|       10th| Mathematics|          3|                  10|
| Doe| 25|    college|      social|          1|                  10|
| Doe| 25|    college|       tamil|          2|                  10|
| Doe| 25|       10th|     English|          1|                  10|
| Doe| 25|       12th| Mathematics|          3|                  10|
| Doe| 25|       12th|     English|          1|                  10|
|John| 25|       10th| Mathematics|          3|                   7|
|John| 25|       12th|     English|          3|                   7|
|John| 25|       10th|     English|          2|                   7|
|John| 25|       10th|     Science|          1|                   7|
|John| 25|       12th|       tamil|          1|                   7|
|John| 25|       12th| Mathematics|          1|                   7|
|John| 25|       12th|      social|          3|                   7|
| Ram| 25|       10th|     English|          2|                   3|
| Ram| 25|       10th| Mathematics|          1|                   3|
| Ram| 25|       10th|     Science|          3|                   3|
+----+---+-----------+------------+-----------+--------------------+
