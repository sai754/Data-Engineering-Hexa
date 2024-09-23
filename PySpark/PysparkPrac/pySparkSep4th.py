```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Employee Data Analysis").getOrCreate()

data = [
    (1, 'Arjun', 'IT', 75000),
    (2, 'Vijay', 'Finance', 85000),
    (3, None, 'IT', 90000),
    (4, 'Sneha', 'HR', None),
    (5, 'Rahul', None, 60000),
    (6, 'Amit', 'IT', 55000)
]

# Define schema
columns = ['EmployeeID', 'EmployeeName', 'Department', 'Salary']

# Create DataFrame
employee_df = spark.createDataFrame(data, columns)

# Show the DataFrame
employee_df.show()

# Fill null values in "EmployeeName" and "Department with "Unknown"
filled_df = employee_df.fillna({"EmployeeName":"Unknown","Department":"Unknown"})
filled_df.show()

# Drop rows where 'Salary' is null
dropped_null_salary_df = employee_df.dropna(subset=["Salary"])
dropped_null_salary_df.show()

# Fill null values in 'Salary' with 50000
salary_filled_df = employee_df.fillna({'Salary':50000})
salary_filled_df.show()

# Check for null values in the entire dataframe
null_counts = employee_df.select([col(c).isNull().alias(c) for c in employee_df.columns]).show()

# Replace all null values in the DataFrame with 'N/A'
na_filled_df = employee_df.na.fill('N/A')
na_filled_df.show()


data1 = [
    (1,'Arjun','IT',75000,'2022-01-15'),
    (2,'Vijay','Finance',85000,'2022-03-12'),
    (3,'Shalini','IT',90000,'2021-06-30')
]

data2 = [
    (4,'Sneha','HR',50000,'2022-05-01'),
    (5,'Rahul','Finance',60000,'2022-08-20'),
    (6,'Amit','IT',55000,'2021-12-15')
]

# Define Schema
columns = ['EmployeeID','EmployeeName','Department','Salary','JoiningDate']

# Create DataFrames
employee_df1 = spark.createDataFrame(data1,columns)
employee_df2 = spark.createDataFrame(data2,columns)

# Show the Dataframes
employee_df1.show()
employee_df2.show()


# Union of two Dataframes (removes Duplicates)
union_df = employee_df1.union(employee_df2).dropDuplicates()
union_df.show()

# Union of two Dataframes (includes Diplicates)
union_all_df = employee_df1.union(employee_df2)
union_all_df.show()

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

# Define a window specifiaction to rank employees by Salary within each department

window_spec = Window.partitionBy("Department").orderBy(col("Salary").desc())

# Add a rank column to the dataframe
ranked_df = union_all_df.withColumn("Rank",rank().over(window_spec))
ranked_df.show()

from pyspark.sql.functions import sum

# define a window specification for cumulative sum of salaries within each department

window_spec_sum = Window.partitionBy("Department").orderBy("JoiningDate").rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Calculate the running total of salaries

running_total_df = union_all_df.withColumn("RunningTotal", sum(col("Salary")).over(window_spec_sum))
running_total_df.show()

# Convert JoiningDate from string to date type
date_converted_df = union_all_df.withColumn("JoiningDate", F.to_date(col("JoiningDate"), "yyyy-MM-dd"))
date_converted_df.show()

# Calculate the number of years since joining
experience_df = date_converted_df.withColumn("YearsOfExperience",F.round(F.datediff(F.current_date(), col("JoiningDate"))/ 365,2))
experience_df.show()

# Add a new column for next evaluation Date (one year after joining)
eval_date_df = date_converted_df.withColumn("NextEvaluationDay", F.date_add(col("JoiningDate"),365))
eval_date_df.show()

# Calculate average salary per department 
avg_salary_df = union_all_df.groupBy("Department").agg(F.avg("Salary").alias("AverageSalary"))
avg_salary_df.show()

# Calculate the total number of employees
total_employees_df = union_all_df.agg(F.count("EmployeeID").alias("TotalEmployees"))
total_employees_df.show()

# Convert empoyee names to upper case
upper_name_df = union_all_df.withColumn("EmployeeNameUpper",F.upper(col("EmployeeName")))
upper_name_df.show()




```