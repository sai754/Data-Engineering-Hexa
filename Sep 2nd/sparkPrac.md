```python
#pip install pyspark
from pyspark.sql import SparkSession

#from pyspark.sql.functions import col

# Initialize SparkSession

spark = SparkSession.builder.appName("PySpark DataFrame Example").getOrCreate()

# Sample data representing employees

data = [

    ("John Doe", "Engineering", 75000),

    ("Jane Smith", "Marketing", 60000),

    ("Sam Brown", "Engineering", 80000),

    ("Emily Davis", "HR", 50000),

    ("Michael Johnson", "Marketing", 70000),

]

# Define schema for DataFrame

columns = ["Name", "Department", "Salary"]

# Create DataFrame

df = spark.createDataFrame(data, schema=columns)

# Show the DataFrame

df.show()

# Filter: Select employees with a salary greater than 65000
high_salary_df = df.filter(col("Salary") > 65000)
print("Employees with Salary > 65000")
high_salary_df.show()

# Group By Department and calculate the average salary
avg_salary_df = df.groupBy("Department").avg("Salary")
print("Average Salary by department")
avg_salary_df.show()


products = [
    (1, "Laptop", "Electronics", 50000),
    (2, "Smartphone", "Electronics", 30000),
    (3, "Table", "Furniture", 15000),
    (4, "Chair", "Furniture", 5000),
    (5, "Headphones", "Electronics", 2000),
]

# Sample data for sales transactions
sales = [
    (1, 1, 2),
    (2, 2, 1),
    (3, 3, 3),
    (4, 1, 1),
    (5, 4, 5),
    (6, 2, 2),
    (7, 5, 10),
    (8, 3, 1),
]

# Define schema for DataFrames
product_columns = ["ProductID", "ProductName", "Category", "Price"]
sales_columns = ["SaleID", "ProductID", "Quantity"]

# Create DataFrames
product_df = spark.createDataFrame(products, schema=product_columns)
sales_df = spark.createDataFrame(sales, schema=sales_columns)

# Show the DataFrames
print("Products DataFrame:")
product_df.show()

print("Sales DataFrame:")
sales_df.show()

# Tasks
# 1. **Join the DataFrames:** 
# Join the `product_df` and `sales_df` DataFrames on `ProductID` to create a combined DataFrame with product and sales data

product_sales_df = product_df.join(sales_df, on="ProductID")
product_sales_df.show()

# Calculate Total Sales Value:**
# For each product, calculate the total sales value by multiplying the price by the quantity sold.

total_sale_product_df = product_sales_df.withColumn("TotalSalesValue", col("Price") * col("Quantity"))
total_sale_product_df.show()

# Find the Total Sales for Each Product Category:**
# Group the data by the `Category` column and calculate the total sales value for each product category.
total_sale_category_df = total_sale_product_df.groupBy("Category").sum("TotalSalesValue").withColumnRenamed("sum(TotalSalesValue)","TotalSales")
total_sale_category_df.show()

# Identify the Top-Selling Product:**
# Find the product that generated the highest total sales value.
total_sale_productName = total_sale_product_df.groupBy("ProductName").sum("TotalSalesValue").withColumnRenamed("sum(TotalSalesValue)","TotalSales")
total_sale_productName.show()

sorted_product_Sale = total_sale_productName.orderBy(col("TotalSales").desc())
top_selling_product = sorted_product_Sale.limit(1)
top_selling_product.show()

# Sort the Products by Total Sales Value:**
# Sort the products by total sales value in descending order.
sorted_product_Sale.show()

# Count the Number of Sales for Each Product:**
# Count the number of sales transactions for each product.

sales_count_df = product_sales_df.groupBy("ProductID").count().withColumnRenamed("count","TransactionCount")
sales_count_df.show()

# Filter the Products with Total Sales Value Greater Than ₹50,000:**
# Filter out the products that have a total sales value greater than ₹50,000.

product_big_price_df = sorted_product_Sale.filter(col("TotalSales") > 50000)
product_big_price_df.show()




```