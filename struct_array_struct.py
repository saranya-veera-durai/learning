input_column_path = 'address.dbo.fullname'
json_data_path = '/content/newly.json'

def generate_pyspark_code(input_column_path, json_data_path):
    parts = input_column_path.split('.')
    
    code_lines = [
        "from pyspark.sql import SparkSession",
        "from pyspark.sql.functions import explode_outer, col\n",
        "spark = SparkSession.builder.appName('DataFrame Operations').getOrCreate()",
        "spark.conf.set('spark.sql.repl.eagerEval.enabled', True)",
        f"df = spark.read.option('multiline', True).json('{json_data_path}')\n",
        "##############################################################################################################",
        f"# {'_'.join(parts)}",
        "##############################################################################################################",
        "try:"
    ]
    
    # Create initial selection line for the root column
    initial_selection = f"    df_{parts[0]} = df.select('name', '{parts[0]}.*')"
    code_lines.append(initial_selection)
    
    # Create lines for nested structures
    previous_name = parts[0]
    alias_name = ""
    for i in range(1, len(parts) - 1):
        current_name = "_".join(parts[:i+1])
        alias_name = parts[i] + "_new"  # Adding 'o' as per the requirement
        
        selection_line = f"    df_{current_name} = df_{previous_name}.select(col('*'), explode_outer('{parts[i]}').alias('{alias_name}'))"
        code_lines.append(selection_line)
        previous_name = current_name
    
    # Final selection line
    final_selection = f"    df_{previous_name} = df_{previous_name}.select(col('*'), '{alias_name}.*', '{alias_name}.{parts[-1]}.*')"
    code_lines.append(final_selection)
    
    # Final DataFrame with drop columns
    final_df_name = f"df_{'_'.join(parts)}_final"
    drop_columns = "', '".join([parts[-2], alias_name, parts[-1]])
    final_selection_line = (
        f"    {final_df_name} = df_{previous_name}.drop('{drop_columns}')"
    )
    code_lines.append(final_selection_line)
    
    # Add show and exception handling
    code_lines.append(f"    {final_df_name}.show()")
    code_lines.append("except Exception as e:")
    code_lines.append(f"    print('Data Not Found in {final_df_name} : ', e)\n")
    code_lines.append("spark.stop()")
    
    return "\n".join(code_lines)

# Generate the PySpark code
generated_code = generate_pyspark_code(input_column_path, json_data_path)

# Optionally, print or save the generated code to a file
print(generated_code)
