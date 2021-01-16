

# List supports Modifications(insert,update) and duplicates and no order
List = list(("a", "bcd", 1, 2, 20, 1.5, 2, 10.0))  # ["a","bcd",1,2,20,1.5,2,10.0]

# Set supports Modifications(insert,update) but not duplicates and stores in an order
Set = set(("a", "bcd", 1, 2, 20, 1.5, 2, 10.0))  # {"a","bcd",1,2,20,1.5,2,10.0}
Set2 = set((1,2,"bcd"))

# tuple doesn't support Modifications but supports indexing
Tuple = tuple(("a", "bcd", 1, 2, 20, 1.5, 2, 10.0))  # ("a","bcd",1,2,20,1.5,2,10.0)

# dictonary supports modifications and accessed using key and values
dictionary = {"name": "rposam", "age": 33, "gender": "Male"}  # {"name":"rposam","age":33,"gender":"Male"}

if __name__ == "__main__":
    print(List.index("bcd")) # Get Index of a value that exists in list
    List.append(564654)  # Add an item to list at the end
    print(List.count(2)) # count of same item in a list
    List.insert(2,"ram posam") # Add item to a list using index
    List.remove("bcd") # Remove item from list using value
    List.pop(0) # Remove item from list using index

    print(List)

    Set.pop()
    Set.remove(1.5)
    Set.add("ram posam") #
    union = Set.union(Set2)
    intersection=Set.intersection(Set2)
    print(union) # union of two tuples
    print(intersection) # intersection of two tuples

    print(Set)

    print(Tuple.index(20)) # index of a value in tuple
    print(Tuple)

    print(dictionary.keys()) # Display all keys of dictonary
    print(dictionary.values()) # Display all values of a dictonary
    print(dictionary.items()) # Dispaly all items of a dictonary
    print(dictionary)


    r = [range(0,20,3)] #
    print(r)


