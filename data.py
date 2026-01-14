import os
import pandas as pd

file_path = os.path.join(
    "Market_60",
    "Nifty",
    "Reliance Industries",
    "RELIANCE_NS_2026_01_14.parquet"
)

df = pd.read_parquet(file_path)
print(df.head())
print(df.tail())
