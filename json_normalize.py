import pandas as pd
import json

# Load JSON from a file
with open('app_life_cycle.json') as f:
    nested_json = json.load(f)

# Flatten the JSON
df = pd.json_normalize(nested_json,"logDataList","_id")

# Display the DataFrame
print(df)

df.to_csv("test.csv", index=False)
