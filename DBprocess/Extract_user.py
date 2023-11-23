import pandas as pd

# File paths
comments_file = "/home/kororu/DMHSS/data/tempstorage/SuicideWatch_comments.csv"
submissions_file = "/home/kororu/DMHSS/data/tempstorage/SuicideWatch_submissions.csv"
output_file = "/home/kororu/DMHSS/DBprocess/user_ids.csv"

# Function to read a CSV file line by line and skip problematic lines
def robust_csv_read(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # Use the header to determine the number of expected columns
    header = lines[0].split(',')
    expected_columns = len(header)
    
    clean_lines = [header]
    for line in lines[1:]:
        if len(line.split(',')) == expected_columns:
            clean_lines.append(line)
    
    return pd.DataFrame([line.split(',') for line in clean_lines[1:]], columns=header)

# Read data with robust error handling
comments_df = robust_csv_read(comments_file)
submissions_df = robust_csv_read(submissions_file)

# Extract unique authors
unique_authors_comments = set(comments_df['author'].unique())
unique_authors_submissions = set(submissions_df['author'].unique())

# Combine and deduplicate
all_unique_authors = unique_authors_comments.union(unique_authors_submissions)

# Convert to DataFrame and save to CSV
authors_df = pd.DataFrame(list(all_unique_authors), columns=["author"])
authors_df.to_csv(output_file, index=False)

print(f"Extracted {len(all_unique_authors)} unique author IDs and saved to {output_file}")
