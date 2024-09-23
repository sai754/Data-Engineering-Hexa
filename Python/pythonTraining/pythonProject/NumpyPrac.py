import numpy as np

# Create a one dimensional array
arr = np.array([1,2,3,4,5])
print(arr)

# Reshape to 2 * 3 array
reshaped_arr = np.arange(6).reshape(2,3)
print("Reshaped array:\n", reshaped_arr)

# Element wise addition
arr_add = arr + 10
print(arr_add)

# Element wise multiplication
arr_mul = arr * 2
print(arr_mul)

# slicing arrays
sliced_arr = arr[1:4] # Get elements from index 1 to 3
print(sliced_arr)