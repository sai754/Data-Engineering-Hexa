# Creating list

empty_list = []
numbers = [1,2,3,4,5]
mixed_list = [1,'Hello',3.14,True]

print(mixed_list)

# Accessing elements
first_element = numbers[0]
third_element = numbers[2]
last_element = numbers[-1]

# Modifying Elements
numbers[0] = 10
numbers[2] = 30

print(numbers)

# Adding items to list
numbers.append(6)

numbers.insert(2,2.5)

numbers.extend([7,8,9])

print(numbers)

# Removing Elements
# numbers.remove(3) # By value

# popped_element = numbers.pop(2) # by index

# Slicing a list
numbers1 = [1,2,3,4,5]

first_three = numbers1[:3]
middle_two = numbers1[1:3]
last_two = numbers1[-2:]

print(first_three)
print(middle_two)
print(last_two)

# Iterating over a list
for num in numbers1:
    print(num)

# List Comprehension
squares = [x**2 for x in range(6)]
print(squares)