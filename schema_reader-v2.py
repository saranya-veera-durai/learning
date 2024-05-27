    # Extract the schema of the DataFrame

import csv


    schema = df_applicantList_final.schema

    # Parse the schema into a list of dictionaries
    schema_data = [
        {"column_name": field.name, "data_type": str(field.dataType), "nullable": field.nullable}
        for field in schema.fields
    ]

    # Specify the path to the output CSV file
    output_path = "/content/df_applicantList_final.csv"

    # Write the schema data to a CSV file
    with open(output_path, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=["column_name", "data_type", "nullable"])
        writer.writeheader()
        writer.writerows(schema_data)
