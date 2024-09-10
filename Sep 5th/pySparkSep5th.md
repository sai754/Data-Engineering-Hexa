```python

from pyspark.sql import SparkSession

# Create a Session
spark = SparkSession.builder.appName("DataIngestion").getOrCreate()

csv_file_path = "/content/sample_data/people.csv"

# Read the file with pyspark
df_csv = spark.read.format("csv").option("header","true").load(csv_file_path)
df_csv.show()


from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define the schema for the JSON file
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("address", StructType([
        StructField("street", StringType(), True),
        StructField("city", StringType(), True)
    ]), True)
])

# Load the complex json file with the correct path
json_file_path = "/content/sample_data/sample.json"

# Read the Json file with schema
df_json_complex = spark.read.schema(schema).json(json_file_path)

# Read the file as text to inspect its contents
with open(json_file_path, 'r') as f:
  data = f.read()
  print(data)

df_multiline_json = spark.read.option("multiline","true").json(json_file_path)
df_multiline_json.show()


from pyspark.sql import SparkSession
import pandas as pd

# Create a Session
spark = SparkSession.builder.appName("CreateViewExample").getOrCreate()

data = {
    "name" : ["John","Jane","Mike","Emily"],
    "age" : [20,32,45,23],
    "gender" : ["Male","Female","Male","Female"],
    "city" : ["New York","San Fransisco","Los Angeles","Chicago"]
}

df = pd.DataFrame(data)

# Save the Data frame toa a csv file
csv_file_path = "/content/sample_people.csv"
df.to_csv(csv_file_path,index=False)

# Confirm the file has been created
print("File has been created")

# Load the csv
df_people = spark.read.format("csv").option("header","true").option("inferSchema","true").load(csv_file_path)

# Show the Data frame
df_people.show()

# Create a temporary view
df_people.createOrReplaceTempView("people_temp_view")

# Run an SQL query on the view
result_temp_view = spark.sql("SELECT name, age, gender, city FROM people_temp_view WHERE age > 30")

# Show the result
result_temp_view.show()

# Create a global temporary view
df_people.createOrReplaceGlobalTempView("people_global_view")

# Query the global temporary view
result_global_view = spark.sql("SELECT name, age, city FROM global_temp.people_global_view WHERE age < 30")

# Show the result
result_global_view.show()

# List all temporary views and tables
spark.catalog.listTables()

# Drop the local temporary view
spark.catalog.dropTempView("people_temp_view")

# Drop the global temporary view
spark.catalog.dropGlobalTempView("people_global_view")


```