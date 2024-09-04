# Creating a set
fruits = {"apple","banana","orange"}

# Adding an element to the set
fruits.add("grape")

# Removing an element from the set
fruits.remove("banana")

# Checking if an element is in the set
print("apple" in fruits)
print("banana" in fruits)

# Length of the set
set_length = len(fruits)
print(set_length)
# Looping through the set
for fruit in fruits:
    print(fruit)
