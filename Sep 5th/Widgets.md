```python
from pyspark.sql import SparkSession
import ipywidgets as widgets
from IPython.display import display
pip install ipywidgets

# Step 1: Initialize a Spark session
spark = SparkSession.builder.appName("PySpark with Widgets Example").getOrCreate()
 
# Step 2: Create a simple DataFrame
data = [
    ("John", 28, "Male", 60000),
    ("Jane", 32, "Female", 72000),
    ("Mike", 45, "Male", 84000),
    ("Emily", 23, "Female", 52000),
    ("Alex", 36, "Male", 67000)
]
 
df = spark.createDataFrame(data, ["name", "age", "gender", "salary"])
 
# Show the DataFrame
df.show()


# Step 3: Create Widgets
# Dropdown widget to select column for filtering
column_dropdown = widgets.Dropdown(
    options=["age","salary"],
    value="age",
    description="Filter By:"
)

# Slider widget to choose a value for filtering
slider = widgets.IntSlider(
    value=30,
    min=20,
    max=100,
    step=5,
    description="Threshold",
    continuos_update = False
)

# Button to trigger filtering
button = widgets.Button(description="Apply Filter")

# Output area to show results
output = widgets.Output()

# Display the widgets
display(column_dropdown,slider,button,output)

# Define the function to apply filtering based on widget inputs
def apply_filter(b):
  column = column_dropdown.value
  threshold = slider.value

  # Clear Previous output
  output.clear_output()

  # Filter the DataFrame based on widget values
  df_filtered = df.filter(df[column] > threshold)

  # Show the filtered DataFrame
  with output:
    print(f"Filtered by {column} > {threshold}")
    df_filtered.show()

# Step 5: Attach the function to the button click event
button.on_click(apply_filter)
```