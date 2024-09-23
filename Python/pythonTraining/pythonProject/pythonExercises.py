# Exercise 1: Create a List
# Create a list called fruits with the following items: "apple", "banana", "cherry", "date", and "elderberry".

fruits = ["apple", "banana", "cherry", "date", "elderberry"]
print(fruits)

# Exercise 2: Access List Elements
# Print the first and last items from the fruits list.
print(f"{fruits[0]}, {fruits[-1]}")

# Print the second and fourth items from the list.
print(f"{fruits[1]},{fruits[3]}")

# Exercise 3: Modify a List
# Replace "banana" in the fruits list with "blueberry".
# Print the modified list.
fruits[1] = "blueberry"
print(fruits)

# Exercise 4: Add and Remove Elements
# Append "fig" and "grape" to the fruits list.
# Remove "apple" from the list.
# Print the final list.
fruits.extend(["fig","grape"])
fruits.remove("apple")
print(fruits)

# Exercise 5: Slice a List
# Slice the first three elements from the fruits list and assign them to a new list called first_three_fruits.
# Print first_three_fruits.

first_three_fruits = fruits[:3]
print(first_three_fruits)

# Exercise 6: Find List Length
# Find and print the length of the fruits list.
print(len(fruits))

# Exercise 7: List Concatenation
# Create a second list called vegetables with the following items: "carrot", "broccoli", "spinach".
# Concatenate the fruits and vegetables lists into a new list called food.
# Print the food list.

vegetables = ["carrot", "broccoli", "spinach"]
food = fruits + vegetables
print(food)

# Exercise 8: Loop Through a List
# Loop through the fruits list and print each item on a new line.

for i in fruits:
    print(i)

# Exercise 9: Check for Membership
# Check if "cherry" and "mango" are in the fruits list. Print a message for each check.

for i in fruits:
    if i == "cherry":
        print("Cherry is present")
    elif i == "mango":
        print("Mango is present")
    else:
        print("Neither cherry nor mango")

# Exercise 10: List Comprehension
# Use list comprehension to create a new list called fruit_lengths that contains the lengths of each item in the fruits list.
# Print the fruit_lengths list.

fruits_length = [len(i) for i in fruits]
print(fruits_length)

# Exercise 11: Sort a List
# Sort the fruits list in alphabetical order and print it.
# Sort the fruits list in reverse alphabetical order and print it.
fruits.sort()
print("Alphabetical Order: ",fruits)
fruits.sort(reverse=True)
print("Reverse Alphabetical: ",fruits)

# Exercise 12: Nested Lists
# Create a list called nested_list that contains two lists: one with the first three fruits and one with the last three fruits.
# Access the first element of the second list inside nested_list and print it.

nested_list = [fruits[:3],fruits[-3:]]
print(nested_list[1][0])

# Exercise 13: Remove Duplicates
# Create a list called numbers with the following elements: [1, 2, 2, 3, 4, 4, 4, 5].
# Remove the duplicates from the list and print the list of unique numbers.

numbers = [1, 2, 2, 3, 4, 4, 4, 5]
unique_nums = []
for i in numbers:
    if i not in unique_nums:
        unique_nums.append(i)
print(unique_nums)

# Exercise 14: Split and Join Strings
# Split the string "hello, world, python, programming" into a list called words using the comma as a delimiter.
# Join the words list back into a string using a space as the separator and print it.

string_sentence = "hello, world, python, programming"

words = string_sentence.split(",")
new_sentence = " ".join(words)
print(new_sentence)