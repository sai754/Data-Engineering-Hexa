import pandas as pd
import numpy as np
# Load the csv file
df = pd.read_csv("sample_data.csv")

print(df)

# # Replace empty strings and string with only spaces with NaN
# df.replace(r'^\s*$', np.nan,regex=True,inplace=True)
#
# # check the null values in each column
# print(df.isnull().sum())
#
# # Display rows with missing data
# print(df[df.isnull().any(axis=1)])
#
# # Drop rows with any missing values
# df_cleaned = df.dropna()
#
# print(df_cleaned)

# Ensure there are no leading/trailing spaces in column names
df.columns = df.columns.str.strip()

# Strip spaces from the 'City' column and replace empty strings with NaN
df["City"] = df["City"].str.strip().replace('',np.nan)



# Fill missing values in the 'City' column with 'Unknown'
df["City"] = df["City"].fillna('Unknown')

# Fill missing values in the 'Age' column with the median age
df["Age"] = pd.to_numeric(df["Age"].str.strip(),errors='coerce')
df["Age"] = df["Age"].fillna(df["Age"].median())

# Fill missing values in the "Salary" column with the median salary
df["Salary"] = df["Salary"].fillna(df["Salary"].median())

print(df)

# Group by
df = pd.DataFrame({
    'employee_id' : [1,2,2,3,3,3],
    'department': ['HR','IT','IT','Finance','Finance','Finance'],
    'salary' : [50000,60000,62000,55000,58000,60000]
})

grouped_df = df.groupby('department')['salary'].mean().reset_index()
print(grouped_df)