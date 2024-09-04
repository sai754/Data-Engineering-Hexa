file_path = "C:/Users/chand/Documents/example.txt"

# Open the file in write mode and write content
with open(file_path, 'w') as file:
    file.write("Hello, this is content written to a file in your laptop")

print("File created and content written successfully")

# Open a file in read mode and print each line
with open(file_path,'r') as file:
    for line in file:
        print(line.strip()) # Remove newline characters

# Open a file in append mode and add new content
with open(file_path,"a") as file:
    file.write("\nThis is additional content appended to the file.")

# Read the entire file as a string and print it
with open(file_path, "r") as file:
    content = file.read()
    print(content)

import csv

# writing data to a csv file
data = [["Name","Age"],["Alice",25],["Bob",30]]
with open("C:/Users/chand/Documents/data.csv","w",newline="") as file:
    writer = csv.writer(file)
    writer.writerows(data)

# Reading data from a CSV file
with open("C:/Users/chand/Documents/data.csv","r") as file:
    reader = csv.reader(file)
    for row in reader:
        print(row)

import json

# Writing data to a JSON file
data = {"name":"Alice", "age": 30,"city":"New York"}
with open("C:/Users/chand/Documents/data.json","w") as file:
    json.dump(data, file)

# Reading data from a JSON file
with open("C:/Users/chand/Documents/data.json","r") as file:
    loaded_data = json.load(file)
    print(loaded_data)
