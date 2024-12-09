input_file = "Texas Last Statement.csv"  
output_file = "Texas Last Statement_utf8.csv"  

# Script If the csv file needs to be changed to UTF-8 (We change the input_file and output_file variables if we want to use a different file)

with open(input_file, 'r', encoding='ISO-8859-1') as source_file:
    with open(output_file, 'w', encoding='UTF-8') as target_file:
        for line in source_file:
            target_file.write(line)

print(f"File has been converted and saved as {output_file}.")