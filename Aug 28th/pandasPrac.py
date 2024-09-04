import pandas as pd

from StringOperations import starts_with_python

# Creating a dataframe from a dictionary
data = {
    "Name":["Amit","Priya","Vikram","Neha","Ravi"],
    "Age":[25,30,35,40,45],
    "City":["Mumbai","Delhi","Bangalore","Chennai","Pune"]
}

df = pd.DataFrame(data)
print(df)

# DataFrame is better than dictionaries because it allows
# to perform various operations easily
# it also gives better clarity about the data while being displayed


# Accessing a single column
# print(df["Name"])
#
# # Accessing multiple columns
# print(df[["Name","Age"]])
#
# # Accessing rows using index
# print(df.iloc[0])

# Filter rows where age is greater tha 30
filtered_df = df[df["Age"] > 30]
print(filtered_df)

# Adding a new column
df["Salary"] = [50000,60000,70000,80000,90000]
print(df)

# Sorting by age
sorted_df = df.sort_values(by="Age",ascending=False)
print(sorted_df)

# Rename the column
df_renamed = df.rename(columns={"Name":"Full Name","Age":"Years"})
print(df_renamed)

# Drop the column
df_dropped = df.drop(columns=["City"])
print(df_dropped)

# # Drop the row
# df_dropped_row = df.drop(index=2)
# print(df_dropped_row)

# Create a new column "Seniority" based on the age
df["Seniority"] = df["Age"].apply(lambda x:'Senior' if x >= 35 else 'Junior')
print(df)

# Group by "City" and calculate the average salary in each city
df_grouped = df.groupby("City")["Salary"].mean()
print(df_grouped)

# Apply a custom function to the salary column to add a 10% bonus
def add_bonus(salary):
    return salary * 1.10
df['Salary_with_Bonus'] = df['Salary'].apply(add_bonus)
print(df)

# Create another DataFrame
df_new = pd.DataFrame({
    "Name": ["Amit","Priya","Ravi"],
    "Bonus": [5000,6000,7000]
})

# Merge based on the 'Name' column
df_merged = pd.merge(df,df_new,on="Name",how="left")
print(df_merged)

# Create another Dataframe to concatenate
df_new_concat = pd.DataFrame({
    "Name":["Sonia","Rahul"],
    "Age":[29,31],
    "City":["Kolkata","Hyderabad"],
    "Salary":[58000,63000]
})

# Concatenate the two dataframes
df_concat = pd.concat([df,df_new_concat],ignore_index=True)
print(df_concat)

# People earning more than 50000
df_salary = df[df["Salary"] > 50000]
print(df_salary)

# People name starting with A
df_name_withA = df[df["Name"].str.startswith('A')]
# names_starting_with_A = df[df['Name'].apply(lambda x: x.startswith('A'))]
print(df_name_withA)