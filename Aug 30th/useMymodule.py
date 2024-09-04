import mymodule
import math_operation
import string_utils
# Modules contain reusable methods
# Package is a collection of module
# from mypackage import arithmetic
# from mypackage import geometry
from mypackage import  *


print(mymodule.greet("Sai"))

print(math_operation.add(1,2))
print(math_operation.sub(3,2))
print(math_operation.mul(10,10))
print(math_operation.div(10,10))

print(string_utils.capitalize_words("hi my name is sai"))
print(string_utils.reverse_string("hello my name is Sai"))
print(string_utils.count_vowels("hello"))
print(string_utils.is_palindrome("malayalam"))

print(arithmetic.add(1,11))
print(geometry.area_of_circle(10))