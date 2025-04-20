def count_occurrences(root_file, search_list):
    
    counts = {word: 0 for word in search_list}
    
    with open(root_file, "r", encoding="utf-8") as file:
        
        for line in file:
            for word in search_list:
                counts[word] += line.count(word)
                
    return counts

# Function
search_list = [" - My Princces ", " - JU4N "]

root_file = "C:/Users/juan_/Downloads/New folder/Validar.txt"

result = count_occurrences(root_file, search_list)

print(result)