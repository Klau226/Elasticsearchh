input_file = "Texas Last Statement.csv"  # Replace with your input file
output_file = "Texas Last Statement_utf8.csv"  # Replace with your desired output file

with open(input_file, 'r', encoding='ISO-8859-1') as source_file:
    with open(output_file, 'w', encoding='UTF-8') as target_file:
        for line in source_file:
            target_file.write(line)

print(f"File has been converted and saved as {output_file}.")