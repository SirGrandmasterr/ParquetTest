import pandas as pd

recommendations = pd.read_parquet("./output/testfile.parquet")

print(recommendations.head())
