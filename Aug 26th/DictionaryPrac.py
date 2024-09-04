# creating dictionaries

empty_dict ={}
person = {
    "name": "Mark",
    "age": 30,
    "email": "mark@example.com"
}

print(person)

# Accessing values
name = person["name"]
age = person["age"]
print(name)
print(age)

# Modifying values
person["age"] = 31
person["email"] = "marknew@example.com"
print(person)

# Adding a new key-value pair
person["address"] = "123 Main St"

# Removing a key-value pair
del person["email"]

print(person)

# Dictionary methods
keys = person.keys()
values = person.values()
items = person.items() # Gives Key value pairs

print(keys)

# Iterating over keys
for key in person:
    print(person[key])

for key in person:
    print(key)

# Iterating over values
for value in person.values():
    print(value)

# Iterating over key,value pairs
for key, value in person.items():
    print(key,":",value)