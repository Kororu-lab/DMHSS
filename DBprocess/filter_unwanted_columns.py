## temp code to get filter out csv to contain only wanted col

import pandas as pd

# Load the CSV file
df = pd.read_csv('filtered_comments_by_subreddit.csv')

# Keep only the desired columns
desired_columns = ['_id', 'id', 'created_utc', 'subreddit', 'author', 'body', 'score', 'parent_id', 'link_id']  # Update as per your needs
df = df[desired_columns]

# Save the modified CSV file
df.to_csv('filtered_comments_by_subreddit_cleaned.csv', index=False)
