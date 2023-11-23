import pandas as pd

NUM_THREADS = 8

all_files = [f"./comments/filtered_comments_{i}.csv" for i in range(NUM_THREADS)]
output_file = "filtered_comments.csv"

# Write headers to the output file
sample_df = pd.read_csv(all_files[0], nrows=0)
sample_df.to_csv(output_file, index=False)

# Append each file to the final CSV without loading the entire dataset into memory
for filename in all_files:
    with open(output_file, 'a') as outfile:
        for line in open(filename, 'r'):
            outfile.write(line)

print(f"Files merged into {output_file}")
