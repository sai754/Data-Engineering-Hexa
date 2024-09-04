# List of dictionaries

students = [
    {"name":"Alice","age":20,"grade":"A"},
    {"name":"Bob","age":22,"grade":"B"},
    {"name":"Charlie","age":21,"grade":"C"},
    {"name":"David","age":23,"grade":"B"},
]

# Accessing
print(students[0]["name"])
students[1]["age"] = 24 # Modifying

# Adding a new student
new_student = {"name":"Eva","age":19,"grade":"A"}
students.append(new_student)

# Iterating through the list of students
for student in students:
    print(student)