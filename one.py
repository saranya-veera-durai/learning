# !pip install pyspark

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

# ----------------------------------------------------------Show()-----------------------------------------------------------

columns = ["Seqno","Quote"]
data = [("1", "Be the change that you wish to see in the world"),
    ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
    ("3", "The purpose of our lives is to be happy."),
    ("4", "Be cool.")]
df = spark.createDataFrame(data,columns)

df.show()
#Display full column contents
df.show(truncate=False)
# Display 2 rows and full column contents
df.show(2,truncate=False) 
# Display 2 rows & column values 25 characters
df.show(2,truncate=25) 
# Display DataFrame rows & columns vertically
df.show(n=3,truncate=25,vertical=True)

from pyspark.sql.functions import col
df.select(col("Quote")).show()

# ----------------------------------------------------------Select()-----------------------------------------------------------

data = [("James","Smith","USA","CA"),
    ("Michael","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
  ]
columns = ["firstname","lastname","country","state"]
df = spark.createDataFrame(data = data, schema = columns)
df.show(truncate=False)


# Select Single & Multiple Columns
df.select("firstname","lastname").show()
df.select(df.firstname,df.lastname).show()
df.select(df["firstname"],df["lastname"]).show()
from pyspark.sql.functions import col
df.select(col("firstname"),col("lastname")).show()


# Select All columns
df.select(*columns).show()
df.select([col for col in df.columns]).show()
df.select("*").show()


# Select Columns by Index
#Selects first 3 columns and top 3 rows
df.select(df.columns[:3]).show(3)
#Selects columns 2 to 4  and top 3 rows
df.select(df.columns[2:4]).show(3)

# ----------------------------------------------------------

#  Select Nested Struct Columns
data = [
        (("James",None,"Smith"),"OH","M"),
        (("Anna","Rose",""),"NY","F"),
        (("Julia","","Williams"),"OH","F"),
        (("Maria","Anne","Jones"),"NY","M"),
        (("Jen","Mary","Brown"),"NY","M"),
        (("Mike","Mary","Williams"),"OH","M")
        ]
from pyspark.sql.types import StructType,StructField, StringType        
schema = StructType([
    StructField('name', StructType([
         StructField('firstname', StringType(), True),
         StructField('middlename', StringType(), True),
         StructField('lastname', StringType(), True)
         ])),
     StructField('state', StringType(), True),
     StructField('gender', StringType(), True)
     ])
df2 = spark.createDataFrame(data = data, schema = schema)
df2.printSchema()
df2.show(truncate=False)
df2.select("name").show(truncate=False)
df2.select("name.firstname","name.lastname").show(truncate=False)
df2.select("name.*").show(truncate=False)

# ----------------------------------------------------------Collect()-----------------------------------------------------------

dept = [("Finance",10),
    ("Marketing",20),
    ("Sales",30),
    ("IT",40)
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(dept,deptColumns)
dataCollect = deptDF.collect()
print(dataCollect) #  [Row(dept_name='Finance', dept_id=10), Row(dept_name='Marketing', dept_id=20), Row(dept_name='Sales', dept_id=30), Row(dept_name='IT', dept_id=40)]
for row in dataCollect:
    print(row['dept_name'] + "," +str(row['dept_id']))
print(dataCollect[0][1]) # 10

dataCollect = deptDF.select("dept_name").collect()
print(dataCollect) #  [Row(dept_name='Finance'), Row(dept_name='Marketing'), Row(dept_name='Sales'), Row(dept_name='IT')]


# ----------------------------------------------------------Withcolumn()-----------------------------------------------------------

from pyspark.sql.functions import col, lit

data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]
columns = ["firstname","middlename","lastname","dob","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)
df.printSchema()
df.show(truncate=False)

df3 = df.withColumn("salary",col("salary")*100).show(truncate=False) 

df5 = df.withColumn("Country", lit("USA")).show(truncate=False) 

df6 = df.withColumn("Country", lit("USA")).withColumn("anotherColumn",lit("anotherValue")).show(truncate=False) 

# ----------------------------------------------------------WithcolumnRenamed()-----------------------------------------------------------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import *

dataDF = [(('James','','Smith'),'1991-04-01','M',3000),
  (('Michael','Rose',''),'2000-05-19','M',4000),
  (('Robert','','Williams'),'1978-09-05','M',4000),
  (('Maria','Anne','Jones'),'1967-12-01','F',4000),
  (('Jen','Mary','Brown'),'1980-02-17','F',-1)
]

schema = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
         StructField('dob', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', IntegerType(), True)
         ])

df = spark.createDataFrame(data = dataDF, schema = schema)
df.printSchema()

# Example 1
df.withColumnRenamed("dob","DateOfBirth").show()
# Example 2   
df2 = df.withColumnRenamed("dob","DateOfBirth").withColumnRenamed("salary","salary_amount").show()
# Example 3 
schema2 = StructType([
    StructField("fname",StringType()),
    StructField("middlename",StringType()),
    StructField("lname",StringType())])
    
df.select(col("name").cast(schema2),  col("dob"),  col("gender")).show()  

# Example 4 
df.select(col("name.firstname").alias("fname"),  col("name.middlename").alias("mname"),  col("name.lastname").alias("lname"),  col("dob"),col("gender"),col("salary")) .show()
  
# Example 5
df4 = df.withColumn("fname",col("name.firstname")) .withColumn("mname",col("name.middlename")) .withColumn("lname",col("name.lastname")).drop("name").show()

#Example 7
newColumns = ["newCol1","newCol2","newCol3","newCol4"]
df.toDF(*newColumns).show()

# ----------------------------------------------------------Filter()-----------------------------------------------------------

from pyspark.sql.types import StructType,StructField 
from pyspark.sql.types import StringType, IntegerType, ArrayType

# Create data
data = [
    (("James","","Smith"),["Java","Scala","C++"],"OH","M"),
    (("Anna","Rose",""),["Spark","Java","C++"],"NY","F"),
    (("Julia","","Williams"),["CSharp","VB"],"OH","F"),
    (("Maria","Anne","Jones"),["CSharp","VB"],"NY","M"),
    (("Jen","Mary","Brown"),["CSharp","VB"],"NY","M"),
    (("Mike","Mary","Williams"),["Python","VB"],"OH","M")
 ]

# Create schema        
schema = StructType([
     StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
         StructField('lastname', StringType(), True)
     ])),
     StructField('languages', ArrayType(StringType()), True),
     StructField('state', StringType(), True),
     StructField('gender', StringType(), True)
 ])

# Create dataframe
df = spark.createDataFrame(data = data, schema = schema)
df.show(truncate=False)
df.filter(df.state == "OH").show(truncate=False)
from pyspark.sql.functions import col
df.filter(col("state") == "OH").show(truncate=False) 
# Not equals condition
df.filter(df.state != "OH").show(truncate=False) 
# Another expression
df.filter(~(df.state == "OH")).show(truncate=False)
# Using SQL Expression
df.filter("gender == 'M'").show()
# For not equal
df.filter("gender != 'M'").show()
df.filter("gender <> 'M'").show()
df.filter( (df.state  == "OH") & (df.gender  == "M") ).show(truncate=False)
df.filter( (df.state  == "OH") | (df.gender  == "M") ).show(truncate=False) 
#  Filter Based on List Values
li=["OH","CA","DE"]
df.filter(df.state.isin(li)).show()   
df.filter(~df.state.isin(li)).show()
df.filter(df.state.isin(li)==False).show()
#  Filter Based on Starts With, Ends With, Contains
df.filter(df.state.startswith("N")).show()
df.filter(df.state.endswith("H")).show()
df.filter(df.state.contains("H")).show()
from pyspark.sql.functions import array_contains
df.filter(array_contains(df.languages,"Java")).show(truncate=False)  
df.filter(df.name.lastname == "Williams").show(truncate=False) 

# ----------------------------------------------------------Drop duplicates()-----------------------------------------------------------

data = [("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("James", "Sales", 4100)
  ]

# Create DataFrame
columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = data, schema = columns)
df.show(truncate=False)
# Applying distinct() to remove duplicate rows
distinctDF = df.distinct()
print("Distinct count: "+str(distinctDF.count()))
distinctDF.show(truncate=False)
# Applying dropDuplicates() to remove duplicate rows
df2 = df.dropDuplicates()
print("Distinct count: "+str(df2.count()))
df2.show(truncate=False)
# Remove duplicates on selected columns using dropDuplicates()
dropDisDF = df.dropDuplicates(["department","salary"])
print("Distinct count of department & salary : "+str(dropDisDF.count()))
dropDisDF.show(truncate=False)


# ----------------------------------------------------------Sort / Orderby()-----------------------------------------------------------


data = [("James","Sales","NY",90000,34,10000), 
    ("Michael","Sales","NY",86000,56,20000), 
    ("Robert","Sales","CA",81000,30,23000), 
    ("Maria","Finance","CA",90000,24,23000), 
    ("Raman","Finance","CA",99000,40,24000), 
    ("Scott","Finance","NY",83000,36,19000), 
    ("Jen","Finance","NY",79000,53,15000), 
    ("Jeff","Marketing","CA",80000,25,18000), 
    ("Kumar","Marketing","NY",91000,50,21000) 
  ]
columns= ["employee_name","department","state","salary","age","bonus"]

df = spark.createDataFrame(data = data, schema = columns)
df.show(truncate=False)
from pyspark.sql.functions import col, asc,desc
df.sort("department","state").show(truncate=False)
df.sort(col("department"),col("state")).show(truncate=False)

df.orderBy("department","state").show(truncate=False)
df.orderBy(col("department"),col("state")).show(truncate=False)

df.sort(df.department.asc(),df.state.asc()).show(truncate=False)
df.sort(col("department").asc(),col("state").asc()).show(truncate=False)
df.orderBy(col("department").asc(),col("state").asc()).show(truncate=False)

df.sort(df.department.asc(),df.state.desc()).show(truncate=False)
df.sort(col("department").asc(),col("state").desc()).show(truncate=False)
df.orderBy(col("department").asc(),col("state").desc()).show(truncate=False)

spark.sql("select employee_name,department,state,salary,age,bonus from EMP ORDER BY department asc").show(truncate=False)

# ----------------------------------------------------------Groupby()-----------------------------------------------------------

from pyspark.sql.functions import col,sum,avg,max

simpleData = [("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  ]

schema = ["employee_name","department","state","salary","age","bonus"]
df = spark.createDataFrame(data=simpleData, schema = schema)
df.printSchema()
df.show(truncate=False)

df.groupBy("department").sum("salary").show(truncate=False)

df.groupBy("department").count().show(truncate=False)

df.groupBy("department","state") .sum("salary","bonus") .show(truncate=False)

df.groupBy("department") \
    .agg(sum("salary").alias("sum_salary"), \
         avg("salary").alias("avg_salary"), \
         sum("bonus").alias("sum_bonus"), \
         max("bonus").alias("max_bonus") \
     ) \
    .show(truncate=False)
    
df.groupBy("department") \
    .agg(sum("salary").alias("sum_salary"), \
      avg("salary").alias("avg_salary"), \
      sum("bonus").alias("sum_bonus"), \
      max("bonus").alias("max_bonus")) \
    .where(col("sum_bonus") >= 50000) \
    .show(truncate=False)


# ----------------------------------------------------------Joins()-----------------------------------------------------------

from pyspark.sql.functions import col

emp = [(1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","10","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (5,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
  ]
empColumns = ["emp_id","name","superior_emp_id","year_joined", \
       "emp_dept_id","gender","salary"]

empDF = spark.createDataFrame(data=emp, schema = empColumns)
empDF.printSchema()
empDF.show(truncate=False)


dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)
  
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner") \
     .show(truncate=False)

empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"outer") \
    .show(truncate=False)
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"full") \
    .show(truncate=False)
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"fullouter") \
    .show(truncate=False)
    
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"left") \
    .show(truncate=False)
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"leftouter") \
   .show(truncate=False)

empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"right") \
   .show(truncate=False)
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"rightouter") \
   .show(truncate=False)

# ----------------------------------------------------------Union()-----------------------------------------------------------


simpleData = [("James","Sales","NY",90000,34,10000), \
    ("Michael","Sales","NY",86000,56,20000), \
    ("Robert","Sales","CA",81000,30,23000), \
    ("Maria","Finance","CA",90000,24,23000) \
  ]

columns= ["employee_name","department","state","salary","age","bonus"]
df = spark.createDataFrame(data = simpleData, schema = columns)
df.printSchema()
df.show(truncate=False)

simpleData2 = [("James","Sales","NY",90000,34,10000), \
    ("Maria","Finance","CA",90000,24,23000), \
    ("Jen","Finance","NY",79000,53,15000), \
    ("Jeff","Marketing","CA",80000,25,18000), \
    ("Kumar","Marketing","NY",91000,50,21000) \
  ]
columns2= ["employee_name","department","state","salary","age","bonus"]

df2 = spark.createDataFrame(data = simpleData2, schema = columns2)

df2.printSchema()
df2.show(truncate=False)

unionDF = df.union(df2)
unionDF.show(truncate=False)
disDF = df.union(df2).distinct()
disDF.show(truncate=False)

# ----------------------------------------------------------UnionByName()-----------------------------------------------------------

# Create DataFrame df1 with columns name, and id
data = [("James",34), ("Michael",56), \
        ("Robert",30), ("Maria",24) ]

df1 = spark.createDataFrame(data = data, schema=["name","id"])
df1.printSchema()

# Create DataFrame df2 with columns name and id
data2=[(34,"James"),(45,"Maria"), \
       (45,"Jen"),(34,"Jeff")]

df2 = spark.createDataFrame(data = data2, schema = ["id","name"])
df2.printSchema()

# Using unionByName()
df3 = df1.unionByName(df2)
df3.printSchema()
df3.show()

# Using allowMissingColumns
df1 = spark.createDataFrame([[5, 2, 6]], ["col0", "col1", "col2"])
df2 = spark.createDataFrame([[6, 7, 3]], ["col1", "col2", "col3"])
df3 = df1.unionByName(df2, allowMissingColumns=True)
df3.printSchema()
df3.show()

# ----------------------------------------------------------aggregate function()-----------------------------------------------------------


simpleData = [("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  ]
schema = ["employee_name", "department", "salary"]
  
df = spark.createDataFrame(data=simpleData, schema = schema)

df2 = df.select(countDistinct("department", "salary"))
df2.show(truncate=False)
print("Distinct Count of Department & Salary: "+str(df2.collect()[0][0]))

print("count: "+str(df.select(count("salary")).collect()[0]))
df.select(first("salary")).show(truncate=False)
df.select(last("salary")).show(truncate=False)
df.select(max("salary")).show(truncate=False)
df.select(min("salary")).show(truncate=False)
df.select(mean("salary")).show(truncate=False)
df.select(sum("salary")).show(truncate=False)

# ----------------------------------------------------------In built functons()-----------------------------------------------------------


# ---------------------------When()-------------------------------

data = [("James","M",60000), ("Michael","M",70000),
        ("Kanchana","Trans",400000), ("Maria","F",500000),
        ("Jen","",None)]

columns = ["name","gender","salary"]
df = spark.createDataFrame(data = data, schema = columns)
df.show()

#Using When otherwise
from pyspark.sql.functions import when,col
df2 = df.withColumn("new_gender", when(df.gender == "M","Male")
                                 .when(df.gender == "F","Female")
                                 .when(df.gender.isNull() ,"")
                                 .otherwise(df.gender))
df2.show()

df2=df.select(col("*"),when(df.gender == "M","Male")
                  .when(df.gender == "F","Female")
                  .when(df.gender.isNull() ,"")
                  .otherwise(df.gender).alias("new_gender"))
df2.show()
# # Using SQL Case When
# from pyspark.sql.functions import expr
# df3 = df.withColumn("new_gender", expr("CASE WHEN gender = 'M' THEN 'Male' " + 
#            "WHEN gender = 'F' THEN 'Female' WHEN gender IS NULL THEN ''" +
#           "ELSE gender END"))
# df3.show()

# df4 = df.select(col("*"), expr("CASE WHEN gender = 'M' THEN 'Male' " +
#            "WHEN gender = 'F' THEN 'Female' WHEN gender IS NULL THEN ''" +
#            "ELSE gender END").alias("new_gender"))

# df.createOrReplaceTempView("EMP")
# spark.sql("select name, CASE WHEN gender = 'M' THEN 'Male' " + 
#                "WHEN gender = 'F' THEN 'Female' WHEN gender IS NULL THEN ''" +
#               "ELSE gender END as new_gender from EMP").show()



# ---------------------------Expr()-------------------------------

from pyspark.sql.functions import expr

#Concatenate columns
data=[("James","Bond"),("Scott","Varsa")] 
df=spark.createDataFrame(data).toDF("col1","col2") 
df.withColumn("Name",expr(" col1 ||','|| col2")).show()

#Using CASE WHEN sql expression
data = [("James","M"),("Michael","F"),("Jen","")]
columns = ["name","gender"]
df = spark.createDataFrame(data = data, schema = columns)
df2 = df.withColumn("gender", expr("CASE WHEN gender = 'M' THEN 'Male' " +
           "WHEN gender = 'F' THEN 'Female' ELSE 'unknown' END"))
df2.show()

#Add months from a value of another column
data=[("2019-01-23",1),("2019-06-24",2),("2019-10-20",3)] 
df=spark.createDataFrame(data).toDF("date","increment") 
df.select(df.date,df.increment,
     expr("add_months(date,increment)")
  .alias("inc_date")).show()
# Providing alias using 'as'
df.select(df.date,df.increment,
     expr("""add_months(date,increment) as inc_date""")
  ).show()

#Add
df.select(df.date,df.increment,
     expr("increment + 5 as new_increment")
  ).show()

#Use expr()  to filter the rows
data=[(100,2),(200,3000),(500,500)] 
df=spark.createDataFrame(data).toDF("col1","col2") 
df.filter(expr("col1 == col2")).show()

# ---------------------------Split()-------------------------------


data = [("James, A, Smith","2018","M",3000),
            ("Michael, Rose, Jones","2010","M",4000),
            ("Robert,K,Williams","2010","M",4000),
            ("Maria,Anne,Jones","2005","F",4000),
            ("Jen,Mary,Brown","2010","",-1)
            ]

columns=["name","dob_year","gender","salary"]
df=spark.createDataFrame(data,columns)
df.show(truncate=False)

from pyspark.sql.functions import split, col
df2 = df.select(split(col("name"),",").alias("NameArray")).show()
df3 = df.withColumn("NameArray", split(col("name"), ",")).drop("name").show()

df.createOrReplaceTempView("PERSON")
spark.sql("select SPLIT(name,',') as NameArray from PERSON").show()


# ---------------------------Concat()-------------------------------


columns = ["name","languagesAtSchool","currentState"]
data = [("James,,Smith",["Java","Scala","C++"],"CA"), \
    ("Michael,Rose,",["Spark","Java","C++"],"NJ"), \
    ("Robert,,Williams",["CSharp","VB"],"NV")]

df = spark.createDataFrame(data=data,schema=columns)
df.show(truncate=False)

from pyspark.sql.functions import col, concat_ws
df2 = df.withColumn("languagesAtSchool",
   concat_ws(",",col("languagesAtSchool")))
df2.show(truncate=False)

df.createOrReplaceTempView("ARRAY_STRING")
spark.sql("select name, concat_ws(',',languagesAtSchool) as languagesAtSchool," + " currentState from ARRAY_STRING").show(truncate=False)

# ---------------------------substring()-------------------------------



from pyspark.sql.functions import col, substring

data = [(1,"20200828"),(2,"20180525")]
columns=["id","date"]
df=spark.createDataFrame(data,columns)

#Using SQL function substring()
df.withColumn('year', substring('date', 1,4))\
    .withColumn('month', substring('date', 5,2))\
    .withColumn('day', substring('date', 7,2))
df.show(truncate=False)

#Using select    
df1=df.select('date', substring('date', 1,4).alias('year'), \
                  substring('date', 5,2).alias('month'), \
                  substring('date', 7,2).alias('day'))
df1.show()
    
#Using with selectExpr
df2=df.selectExpr('date', 'substring(date, 1,4) as year', \
                  'substring(date, 5,2) as month', \
                  'substring(date, 7,2) as day')
df2.show()

#Using substr from Column type
df3=df.withColumn('year', col('date').substr(1, 4))\
  .withColumn('month',col('date').substr(5, 2))\
  .withColumn('day', col('date').substr(7, 2))

df3.show()


# ---------------------------regexp_replace()-------------------------------
 

address = [(1,"14851 Jeffrey Rd","DE"),
    (2,"43421 Margarita St","NY"),
    (3,"13111 Siemon Ave","CA")]

df =spark.createDataFrame(address,["id","address","state"])
df.show()

#Replace string
from pyspark.sql.functions import regexp_replace
df.withColumn('address', regexp_replace('address', 'Rd', 'Road')) \
  .show(truncate=False)

#Replace string
from pyspark.sql.functions import when
df.withColumn('address', 
      when(df.address.endswith('Rd'),regexp_replace(df.address,'Rd','Road')) \
     .when(df.address.endswith('St'),regexp_replace(df.address,'St','Street')) \
     .when(df.address.endswith('Ave'),regexp_replace(df.address,'Ave','Avenue')) \
     .otherwise(df.address)) \
     .show(truncate=False)   

#Replace values from Dictionary
stateDic={'CA':'California','NY':'New York','DE':'Delaware'}
df2=df.rdd.map(lambda x: 
    (x.id,x.address,stateDic[x.state]) 
    ).toDF(["id","address","state"])
df2.show()

#Using translate
from pyspark.sql.functions import translate
df.withColumn('address', translate('address', '123', 'ABC')) \
  .show(truncate=False)


# ---------------------------to_timestamp()-------------------------------


df=spark.createDataFrame(
        data = [ ("1","2019-06-24 12:01:19.000")],
        schema=["id","input_timestamp"])
df.printSchema()


from pyspark.sql.functions import *

# Using Cast to convert Timestamp String to DateType
df.withColumn('date_type', col('input_timestamp').cast('date')).show(truncate=False)
# Using Cast to convert TimestampType to DateType
df.withColumn('date_type', to_timestamp('input_timestamp').cast('date')).show(truncate=False)
#Timestamp String to DateType
df.withColumn("date_type",to_date("input_timestamp")).show(truncate=False)
#Timestamp Type to DateType
df.withColumn("date_type",to_date(current_timestamp())).show(truncate=False) 
df.withColumn("ts",to_timestamp(col("input_timestamp"))).withColumn("datetype",to_date(col("ts"))).show(truncate=False)


#SQL TimestampType to DateType
spark.sql("select to_date(current_timestamp) as date_type").show()
#SQL CAST TimestampType to DateType
spark.sql("select date(to_timestamp('2019-06-24 12:01:19.000')) as date_type").show()
#SQL CAST timestamp string to DateType
spark.sql("select date('2019-06-24 12:01:19.000') as date_type").show()
#SQL Timestamp String (default format) to DateType
spark.sql("select to_date('2019-06-24 12:01:19.000') as date_type").show()
#SQL Custom Timeformat to DateType
spark.sql("select to_date('06-24-2019 12:01:19.000','MM-dd-yyyy HH:mm:ss.SSSS') as date_type").show()


# ---------------------------date_format()-------------------------------

df.select(current_date().alias("current_date"), \
      date_format(current_date(),"yyyy MM dd").alias("yyyy MM dd"), \
      date_format(current_timestamp(),"MM/dd/yyyy hh:mm").alias("MM/dd/yyyy"), \
      date_format(current_timestamp(),"yyyy MMM dd").alias("yyyy MMMM dd"), \
      date_format(current_timestamp(),"yyyy MMMM dd E").alias("yyyy MMMM dd E") \
   ).show()

#SQL

spark.sql("select current_date() as current_date, "+
      "date_format(current_timestamp(),'yyyy MM dd') as yyyy_MM_dd, "+
      "date_format(current_timestamp(),'MM/dd/yyyy hh:mm') as MM_dd_yyyy, "+
      "date_format(current_timestamp(),'yyyy MMM dd') as yyyy_MMMM_dd, "+
      "date_format(current_timestamp(),'yyyy MMMM dd E') as yyyy_MMMM_dd_E").show()

# ---------------------------datediff()-------------------------------

from pyspark.sql.functions import col, current_date, datediff, months_between, round, lit, to_date
# Create DataFrame
data = [("1","2019-07-01"),("2","2019-06-24"),("3","2019-08-24")]
df=spark.createDataFrame(data=data,schema=["id","date"])

# Calculate the difference between two dates
df2 = df.select(
      col("date"),
      current_date().alias("current_date"),
      datediff(current_date(),col("date")).alias("datediff")
    )
df2.show()
# Calculate the difference between two dates in months
df3 = df.withColumn("datesDiff", datediff(current_date(), col("date"))) \
  .withColumn("monthsDiff", months_between(current_date(), col("date"))) \
  .withColumn("monthsDiff_round", round(months_between(current_date(), col("date")), 2))
  
df3.show()

df4 = df.withColumn("datesDiff", datediff(current_date(), col("date"))) \
  .withColumn("yearsDiff", months_between(current_date(), col("date")) / lit(12)) \
  .withColumn("yearsDiff_round", round(months_between(current_date(), col("date")) / lit(12), 2))
df4.show()


# ---------------------------explode() flatten()-------------------------------


arrayArrayData = [
  ("James",[["Java","Scala","C++"],["Spark","Java"]]),
  ("Michael",[["Spark","Java","C++"],["Spark","Java"]]),
  ("Robert",[["CSharp","VB"],["Spark","Python"]])
]

df = spark.createDataFrame(data=arrayArrayData, schema = ['name','subjects'])
df.show(truncate=False)

from pyspark.sql.functions import explode
df.select(df.name,explode(df.subjects)).show(truncate=False)

from pyspark.sql.functions import flatten
df.select(df.name,flatten(df.subjects)).show(truncate=False)


# ---------------------------split array array_contains-------------------------------


data = [
 ("James,,Smith",["Java","Scala","C++"],["Spark","Java"],"OH","CA"),
 ("Michael,Rose,",["Spark","Java","C++"],["Spark","Java"],"NY","NJ"),
 ("Robert,,Williams",["CSharp","VB"],["Spark","Python"],"UT","NV")
]

from pyspark.sql.types import StringType, ArrayType,StructType,StructField
schema = StructType([ 
    StructField("name",StringType(),True), 
    StructField("languagesAtSchool",ArrayType(StringType()),True), 
    StructField("languagesAtWork",ArrayType(StringType()),True), 
    StructField("currentState", StringType(), True), 
    StructField("previousState", StringType(), True)
  ])

df = spark.createDataFrame(data=data,schema=schema)
df.show()

from pyspark.sql.functions import split
df.select(split(df.name,",").alias("nameAsArray")).show()

from pyspark.sql.functions import array
df.select(df.name,array(df.currentState,df.previousState).alias("States")).show()

from pyspark.sql.functions import array_contains
df.select(df.name,array_contains(df.languagesAtSchool,"Java").alias("array_contains")).show()

# ---------------------------#Convert columns to Map------------------------------


from pyspark.sql.types import StructType,StructField, StringType, IntegerType

data = [ ("36636","Finance",3000,"USA"), 
    ("40288","Finance",5000,"IND"), 
    ("42114","Sales",3900,"USA"), 
    ("39192","Marketing",2500,"CAN"), 
    ("34534","Sales",6500,"USA") ]
schema = StructType([
     StructField('id', StringType(), True),
     StructField('dept', StringType(), True),
     StructField('salary', IntegerType(), True),
     StructField('location', StringType(), True)
     ])

df = spark.createDataFrame(data=data,schema=schema)
df.show(truncate=False)
from pyspark.sql.functions import col,lit,create_map
df = df.withColumn("propertiesMap",create_map(
        lit("salary"),col("salary"),
        lit("location"),col("location")
        )).drop("salary","location")
df.printSchema()
df.show(truncate=False)


# ---------------------------map_keys,map_values------------------------------


from pyspark.sql.types import StringType, ArrayType,StructType,StructField,MapType
dataDictionary = [
        ('James',{'hair':'black','eye':'brown'}),
        ('Michael',{'hair':'brown','eye':None}),
        ('Robert',{'hair':'red','eye':'black'}),
        ('Washington',{'hair':'grey','eye':'grey'}),
        ('Jefferson',{'hair':'brown','eye':''})
        ]

# Define schema for DataFrame
schema = StructType([
    StructField("name", StringType(), True),
    StructField("parts", MapType(StringType(), StringType()), True)
])
df = spark.createDataFrame(data=dataDictionary, schema = schema)

from pyspark.sql.functions import map_keys,map_values

# Extract keys and values
df_keys = df.select("name", map_keys(df.parts).alias("part_keys"))
df_values = df.select("name", map_values(df.parts).alias("part_values"))

# Show the results
df_keys.show(truncate=False)
df_values.show(truncate=False)

# ---------------------------countDistinct------------------------------


data = [("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  ]
columns = ["Name","Dept","Salary"]
df = spark.createDataFrame(data=data,schema=columns)
df.distinct().show()
print("Distinct Count: " + str(df.distinct().count()))

# Using countDistrinct()
from pyspark.sql.functions import countDistinct
df2=df.select(countDistinct("Dept","Salary"))
df2.show()

# --------------------------windows-----------------------------


simpleData = (("James", "Sales", 3000), \
    ("Michael", "Sales", 4600),  \
    ("Robert", "Sales", 4100),   \
    ("Maria", "Finance", 3000),  \
    ("James", "Sales", 3000),    \
    ("Scott", "Finance", 3300),  \
    ("Jen", "Finance", 3900),    \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000),\
    ("Saif", "Sales", 4100) \
  )
 
columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = simpleData, schema = columns)
df.printSchema()
df.show(truncate=False)

# row_number() example
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
windowSpec  = Window.partitionBy("department").orderBy("salary")

df.withColumn("row_number",row_number().over(windowSpec)) \
    .show(truncate=False)

# rank() example
from pyspark.sql.functions import rank
df.withColumn("rank",rank().over(windowSpec)) \
    .show()

# dense_rank() example 
from pyspark.sql.functions import dense_rank
df.withColumn("dense_rank",dense_rank().over(windowSpec)) \
    .show()

from pyspark.sql.functions import lag    
df.withColumn("lag",lag("salary",2).over(windowSpec)) \
      .show()

from pyspark.sql.functions import lead    
df.withColumn("lead",lead("salary",2).over(windowSpec)) \
    .show()


# --------------------------from json-----------------------------


jsonString="""{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}"""
df=spark.createDataFrame([(1, jsonString)],["id","value"])
df.show(truncate=False)




from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create Spark session
spark = SparkSession.builder.appName("JsonExample").getOrCreate()

# Define schema for the JSON data
schema = StructType([
    StructField("name", StringType()),
    StructField("age", IntegerType())
])

# Create sample data
data = [("1", '{"name": "John", "age": 30}'), ("2", '{"name": "Jane", "age": 25}')]

# Create DataFrame
df = spark.createDataFrame(data, ["id", "json_string"])

# Convert JSON string to struct
df_parsed = df.withColumn("parsed", from_json("json_string", schema))
df_parsed.show()

# Convert struct back to JSON string
df_to_json = df_parsed.withColumn("json", to_json(col("parsed")))
df_to_json.show()

from pyspark.sql.functions import json_tuple

# Using the same DataFrame from the previous example
df_tuple = df.select("id", json_tuple("json_string", "name", "age").alias("name", "age"))
df_tuple.show()

from pyspark.sql.functions import get_json_object

# Extracting a specific element using JSON path
df_json_object = df.select(
    "id",
    get_json_object("json_string", "$.name").alias("name"),
    get_json_object("json_string", "$.age").alias("age")
)
df_json_object.show()


# --------------------------fillna----------------------------


# Create DataFrame
data = [("James", None, 34),
        ("Anna", "Female", None),
        ("Julia", None, None)]
columns = ["Name", "Gender", "Age"]

df = spark.createDataFrame(data, columns)
df.show()

# Replace all null values with a specific value
df_filled = df.fillna({"Age":10})
df_filled.show()

# Replace nulls in specific columns
df_filled_specific = df.fillna({"Gender": "Not specified", "Age": 0})
df_filled_specific.show()


# --------------------------read csv----------------------------


from google.colab import files
uploaded = files.upload()

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
df = spark.read.csv("sampledata.csv", header=True, inferSchema=True)
df.show()