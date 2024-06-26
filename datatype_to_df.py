input_list = ["customObject=struct", "vendor=array", "sSubmittedDate=date", "collateralList=array", "ownerNames=array", "apiMetaData=struct"]

pyspark_code = []
prev_df = "df"

def generate_code(input_list):
    pyspark_code = []
    prev_df = "df"
    
    for item in input_list:
        col_name, col_type = item.split('=')
        if col_type == 'array':
            new_df = f"{prev_df}_{col_name}"
            pyspark_code.append(f"{new_df} = {prev_df}.withColumn('{col_name}', explode(col('{col_name}')))")
        elif col_type == 'struct':
            new_df = f"{prev_df}_{col_name}"
            pyspark_code.append(f"{new_df} = {prev_df}.select('*', col('{col_name}.*'))")
        elif col_type == 'date':
            new_df = f"{prev_df}"
            pyspark_code.append(f"{new_df} = transform_timestamp_split({prev_df}, '{col_name}')")
        prev_df = new_df
    
    return pyspark_code

pyspark_code = generate_code(input_list)

for line in pyspark_code:
    print(line)
