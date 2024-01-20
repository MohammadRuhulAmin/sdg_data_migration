information = [
    [1, 2, 'Category A'],
    [2, 3, 'Category B'],
    [3, 2, 'Category B'],
    [1, 2, 'Category A'],
    [4, 5, 'Category C'],
    [4, 5, 'Category B'],
    [2, 3, 'Category B']
]

# Convert each sublist to a tuple and use a set to track unique tuples
unique_tuples_set = set(tuple(subarray) for subarray in information)

# Create a new array with unique subarrays
unique_information = [list(t) for t in unique_tuples_set]

# Display the result
print(unique_information)
