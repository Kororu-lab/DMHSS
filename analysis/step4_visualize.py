import pandas as pd
from pathlib import Path

# Load the data
file_path = Path('./step4/user_interaction_scores.csv')  # Adjust the file path as needed
df = pd.read_csv(file_path)

# Print the first few rows of the dataframe
print(df.head())

# Check the structure of the data
print("\nData columns:")
print(df.columns)

# Check for any NaN values in the month and score columns
print("\nNaN values in the 'month' column:")
print(df['month'].isna().sum())

print("\nNaN values in the 'score' column:")
print(df['score'].isna().sum())
