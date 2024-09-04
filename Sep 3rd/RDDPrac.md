```python
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("RDD Transformation").getOrCreate()

# Get the SparkContext from the SparkSession
sc = spark.sparkContext
print("Spark Session Created")

data = [1,2,3,4,5,6,7,8,9,10]
rdd = sc.parallelize(data)

# Print the original RDD
print("Original RDD", rdd.collect())

# Map
rdd2 = rdd.map(lambda x: x*2)
# Print Transformed RDD
print("Transformed RDD:", rdd2.collect())

# Filter
rdd3 = rdd2.filter(lambda x:x % 2 == 0)
print(rdd3.collect())

# FlatMap
sentences = ["Hello World", " PySpark is great", "RDD transformations"]
rdd4 = sc.parallelize(sentences)
words_rdd = rdd4.flatMap(lambda sentence: sentence.split(" "))

print(words_rdd.collect())

# Actions
# Collect
results = rdd3.collect()
print(results)

# Count
count = rdd3.count()
print(f"The number of elements: {count}")

# Reduce
total_sum = rdd.reduce(lambda x, y: x + y)
print(f"Total sum: {total_sum}")
```

