import os
import re

directory = r"c:\Users\Mahdiar\Desktop\Negar genes"  # Update this path accordingly

for idx, file_name in enumerate(os.listdir(directory)):
    print(f"Original file name: {file_name}")

    # Creating the new file name by replacing spaces and removing commas
    gene_name = file_name.split(".")[0]
    new_file_name = f"{gene_name}_{idx}.fasta"

    # Build the full paths for old and new names
    old_file_path = os.path.join(directory, file_name)
    new_file_path = os.path.join(directory, new_file_name)

    # Check if the file exists before renaming
    if os.path.exists(old_file_path):
        print(f"Renaming: {old_file_path} to {new_file_path}")
        os.rename(old_file_path, new_file_path)
    else:
        print(f"File not found: {old_file_path}")

    # Open and read the content of the file
    with open(new_file_path, 'r') as file:
        content = file.read()

    # Define the regex pattern to match sequences of 10 or more uppercase English letters
    pattern = r'[A-Z]{10,}'

    # Find all sequences that match the pattern
    matches = re.findall(pattern, content)

    # Write the matches into a new file
    with open(new_file_path, 'w') as output_file:
        for match in matches:
            output_file.write(match + '\n')

    print("The sequences have been extracted and saved to 'matches.txt'.")
