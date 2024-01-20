information = [
    [1, 2, 'Category A'],
    [2, 3, 'Category B'],
    [3, 2, 'Category B'],
    [1, 2, 'Category A'],
    [4, 5, 'Category C'],
    [4, 5, 'Category B'],
    [2, 3, 'Category B']
]

def unique_nested_array(information):
    try:
        unique_tuples_set = set(tuple(subarray) for subarray in information)
        unique_information = [list(t) for t in unique_tuples_set]
        return unique_information
    except Exception as E:
        return str(E)


print(unique_nested_array(information))