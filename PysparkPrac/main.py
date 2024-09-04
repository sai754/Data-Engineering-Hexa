# pip install pyspark
# from pyspark.sql import SparkSession
#
# from pyspark.sql.functions import col
#
# # Initialize SparkSession
#
# spark = SparkSession.builder.appName("PySpark DataFrame Example").getOrCreate()
#
# # Sample data representing employees
#
# data = [
#
#     ("John Doe", "Engineering", 75000),
#
#     ("Jane Smith", "Marketing", 60000),
#
#     ("Sam Brown", "Engineering", 80000),
#
#     ("Emily Davis", "HR", 50000),
#
#     ("Michael Johnson", "Marketing", 70000),
#
# ]
#
# # Define schema for DataFrame
#
# columns = ["Name", "Department", "Salary"]
#
# # Create DataFrame
#
# df = spark.createDataFrame(data, schema=columns)
#
# # Show the DataFrame
#
# df.show()
#
# Filter: Select employees with a salary greater than 65000
# high_salary_df = df.filter(col("Salary") > 65000)
# print("Employees with Salary > 65000")
# high_salary_df.show()
#
# # Group By Department and calculate the average salary
# avg_salary_df = df.groupBy("Department").avg("Salary")
# print("Average Salary by department")
# avg_salary_df.show()