import csv
import sys
from tqdm import tqdm

# Increase the field size limit
csv.field_size_limit(sys.maxsize)

input_file = "filtered_submissions.csv"
output_file = "sorted_filtered_submissions.csv"

# Function to get the 'created_utc' column value for sorting
def get_utc(row):
    return int(row[1])  # Assuming 'created_utc' is the second column

rows = []

with open(input_file, 'r') as f:
    reader = csv.reader(f)
    headers = next(reader)  # Read the header
    for row in tqdm(reader, desc="Reading rows", unit="rows"):
        rows.append(row)

# Sort the rows by 'created_utc' column
rows.sort(key=get_utc)

# Write the sorted rows to the output file
with open(output_file, 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(headers)
    writer.writerows(rows)

print(f"File sorted by 'created_utc' and saved to {output_file}")
