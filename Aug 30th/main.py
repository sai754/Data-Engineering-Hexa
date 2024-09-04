# print("Hello world")

single_quote_string = 'This is single quote String - "Hey"'

double_quote_string = "This is double quote String"

multi_line = '''This is a
Multi line string
'''
# print(single_quote_string)
# print(double_quote_string)
# print(multi_line)

import pandas as pd
# Load the CSV file into a dataframe
df = pd.read_csv('employee.csv')
print(df)

# Display the first 3 rows
print(df.head(3))

# Show summary information about the data frame
print(df.info())

# Display summary statistics of numeric columns
print(df.describe())

# Filter rows where Salary is greater than 80000
higher_salary_df = df[df["Salary"] > 80000]
print(higher_salary_df)

# Sort by age in desc
sorted_df_Age = df.sort_values(by="Age",ascending=False)
print(sorted_df_Age)

# Load the JSON file into dataframe
json_df = pd.read_json('employees.json')
print(json_df)

# Add a new column 'Bonus' which is 10% of the salary
json_df['Bonus'] = json_df['Salary'] * 0.10
print(json_df)

# Save the updated Dataframe to a new JSON file
df.to_json('employees_with_bonus.json',orient='records',lines=True)

# Keyword Arguments
def describe_pet(pet_name, animal_type="dog"):
    print(f"I have a {animal_type} named {pet_name}")

describe_pet(animal_type="cat",pet_name="Pichu")
describe_pet(pet_name="Rover")

# Arbitrary arguments
def make_pizza(size, *toppings):
    print(f"Making a {size}-inch pizza with the toppings:")
    for topping in toppings:
        print(f"- {topping}")

make_pizza(12,"pepperoni","mushrooms","green peppers")

# Arbitrary keyword arguments
def build_portfolio(first,last,**user_info):
    return {"first_name":first,"last_name":last,**user_info}

user_profile = build_portfolio("john","doe",location="New York", field="Engineering")
print(user_profile)