import csv
import sys
from tqdm import tqdm

# Increase the field size limit
csv.field_size_limit(sys.maxsize)

expected_fields = 7

with open("filtered_comments_total.csv", 'r') as file:
    reader = csv.reader(file)
    for row in tqdm(reader, desc="Checking rows", unit="rows"):
        if len(row) != expected_fields:
            print(f"Unexpected row length: {','.join(row)}")

print("Finished checking file.")
