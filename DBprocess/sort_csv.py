import pandas as pd

# File paths
input_file = "filtered_comments.csv"
output_file = "sorted_filtered_comments.csv"

# Load the CSV into a DataFrame
df = pd.read_csv(input_file)

# Sort the DataFrame by the 'created_utc' column
df_sorted = df.sort_values(by='created_utc')

# Save the sorted DataFrame back to CSV
df_sorted.to_csv(output_file, index=False)

print(f"File sorted by 'created_utc' and saved to {output_file}")
