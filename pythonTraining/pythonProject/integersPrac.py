positive_int = 3
negative_int = -3
zero = 0

print(positive_int)
print(negative_int)
print(zero)

a = 10
b = 3
# Addition
addition = a + b
# Subtraction
subtraction = a - b
# Multiplication
multiplication = a * b
# Division
division = a / b # Returns a float value
# Floor Division
floor_division = a // b # Returns a integer value
# Modulus
modulus = a % b
# Exponent
exponentiation = a ** b

print("Addition: ",addition)
print("Subtraction: ",subtraction)
print("Multiplication: ",multiplication)
print("Division: ",division)
print("Floor Division: ",floor_division)
print("Modulus: ",modulus)
print("Exponentiation: ",exponentiation)

# Type Casting
# String to Integer
num_str = "100"
num_int = int(num_str)

# Float to Integer
num_float = 12.34
num_int_from_float = int(num_float)

print("String to Integer: ", num_int)
print("Float to integer: ", num_int_from_float )

# Comparison Operators
x = 10
y = 5

is_greater = x > y # True
is_equal = x == y # False
print("x > y :", is_greater)
print("x == y :",is_equal)

# Logical Operators
a = True
b = False

# Logical And
and_operation = a and b # False

# Logical Or
or_operation = a or b # True

# Logical Not
not_operation = not a # False

print("a and b: ",and_operation)
print("a or b: ",or_operation)
print("not a: ", not_operation)

# Convert integer to boolean
bool_from_int = bool(1) # True

# Convert zero to boolean
bool_from_zero = bool(0) # False

# Convert String to boolean
bool_from_str = bool("Hello") # True

# Convert empty string to boolean
bool_from_empty_str = bool("") # False