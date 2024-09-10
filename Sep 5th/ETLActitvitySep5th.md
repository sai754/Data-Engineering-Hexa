```python
import pandas as pd
from pyspark.sql.functions import col
from pyspark.sql import functions as F

# Create a sample CSV data
data = {
    "name": ["John", "Jane", "Mike", "Emily", "Alex"],
    "age": [28, 32, 45, 23, 36],
    "gender": ["Male", "Female", "Male", "Female", "Male"],
    "salary": [60000, 72000, 84000, 52000, 67000]
}
 
df = pd.DataFrame(data)
 
# Save the DataFrame as a CSV file
csv_file_path = "/content/sample_people1.csv"
df.to_csv(csv_file_path, index=False)
 
# Confirm the CSV file is created
print(f"CSV file created at: {csv_file_path}")


df_etl = spark.read.format("csv").option("header","true").option("inferSchema","true").load(csv_file_path)

# Show the Data frame
df_etl.show()

# 2. **Transform**:
#    - **Filter**: Only include employees aged 30 and above in the analysis.

df_etl = df_etl.filter(col("age") > 30)
df_etl.show()

#    - **Add New Column**: Calculate a 10% bonus on the current salary for each employee and add it as a new column (`salary_with_bonus`).

df_etl = df_etl.withColumn("salary_with_bonus", col("salary") * 1.10)
df_etl.show()

#    - **Aggregation**: Group the employees by gender and compute the average salary for each gender.

df_group_etl = df_etl.groupBy("gender").agg(F.avg("salary").alias("average_salary"))
df_group_etl.show()

# 3. **Load**:
#    - Save the transformed data (including the bonus salary) in a Parquet file format for efficient storage and retrieval.
#    - Ensure the data can be easily accessed for future analysis or reporting.

df_etl.write.parquet("/content/people.parquet")

df_group_etl.write.parquet("/content/gender_average.parquet")

# Full Refresh

from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.functions import col
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("SparkSQLExample").getOrCreate()


# Full Refresh: Load the dataset
df_sales = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/content/sample_data/sales_data.csv")

# Apply transformations (if necessary)
df_transformed = df_sales.withColumn("total_sales",df_sales["quantity"] * df_sales["price"])

# Full Refresh: Partition the data by 'date' and overwrite the existing data
output_path = '/content/sample_data/partitioned_data'
df_transformed.write.partitionBy("date").mode("overwrite").parquet(output_path)
# For Incremental approach change mode to "append"

# Verify partitioned data
partitioned_df = spark.read.parquet(output_path)
partitioned_df.show()







```