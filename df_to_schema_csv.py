
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode_outer, col

spark = SparkSession.builder.appName('DataFrame Operations').getOrCreate()
spark.conf.set('spark.sql.repl.eagerEval.enabled', True)
df = spark.read.option('multiline', True).json('/content/tested.json')

# ##############################################################################################################
# # applicantList
# ##############################################################################################################
try:
    df_applicantList = df.select('_id', explode_outer('applicantList').alias('applicantList'))
    df_applicantList_final = df_applicantList.select('_id', 'applicantList.*', 'applicantList.applicantName.*', 'applicantList.fatherName.*', 'applicantList.motherName.*', 'applicantList.professionIncomeDetails.*', 'applicantList.spouseName.*', 'applicantList.thirdPartyCall.*')
    df_applicantList_final.show()

    schema = df_applicantList_final.schema

    # Parse the schema into a list of dictionaries
    schema_data = [
        {"column_name": field.name, "data_type": str(field.dataType), "nullable": field.nullable}
        for field in schema.fields
    ]

    # Create a DataFrame from the schema data
    schema_df = spark.createDataFrame(schema_data).select(
        col("column_name"),
        col("data_type"),
        col("nullable")
    )

    # Write the schema DataFrame to a CSV file
    output_path = "/content/schema"  # Specify the path to the output CSV file
    schema_df.write.option("header", "true").csv(output_path+".csv")

except Exception as e:
    print('Data Not Found in df_applicantList_address : ', e)

spark.stop()
