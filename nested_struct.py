def generate_pyspark_code(column_paths):
    code_blocks = []
    for path in column_paths:
        path_sanitized = path.replace("/","").replace("-","").replace("_","").replace('.', '_')
        code_block = f"""
# #######################################################################################################################################
# {path}
# #######################################################################################################################################
try:
    df_{path_sanitized} = df.select("_id", "{path}.*")
    df_{path_sanitized}.show()
except Exception as e:
    print("Data Not Found in df_{path_sanitized} : ", e)
"""
        code_blocks.append(code_block)
    
    return "\n".join(code_blocks)

# List of remaining paths
column_paths = [
    "flatAppLifeCycle.submitData.call-applicant-dedupe",
    "flatAppLifeCycle.submitData.call-applicant-dedupe.changedData",
]


# Generate the PySpark code
generated_code = generate_pyspark_code(column_paths)

# Write the generated code to a Python file
with open("generated_pyspark_code.py", "w") as file:
    file.write(generated_code)
