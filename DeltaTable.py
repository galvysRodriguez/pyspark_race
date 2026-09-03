from deltalake import DeltaTable
import pandas as pd

# 1. Point to the local directory where PySpark wrote the Delta table
table_path = "./storage/delta/race_leaderboard"

# 2. Load the Delta table state
dt = DeltaTable(table_path)

# 3. Convert directly to a pandas DataFrame
df = dt.to_pandas()

# Display the first few rows
print(df.head())