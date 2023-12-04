import pandas as pd
import os

def perform_aggregation_and_analysis(input_file, output_file):
    try:
        df = pd.read_csv(input_file)
        aggregated_df = df.groupby(['user_group']).agg({
            'num_nouns': 'mean',
            'num_verbs': 'mean',
            'num_adverbs': 'mean',
            'first_person_pronouns': 'mean',
            'second_person_pronouns': 'mean',
            'third_person_pronouns': 'mean',
            'her_count': 'mean',
            'readability_score': 'mean'
        }).reset_index()

        # Perform additional statistical analysis as needed

        # Save the aggregated data to a new CSV file
        aggregated_df.to_csv(output_file, index=False)
        print(f"Aggregated data saved to {output_file}")
    except Exception as e:
        print(f"An error occurred: {e}")

# File paths
input_dir = "./step2/"
output_dir = "./step2/aggregated_results/"

# Ensure the output directory exists
os.makedirs(output_dir, exist_ok=True)

# Perform aggregation and analysis on the results
perform_aggregation_and_analysis(os.path.join(input_dir, "submission_results.csv"), 
                                 os.path.join(output_dir, "submission_results_aggregated.csv"))

perform_aggregation_and_analysis(os.path.join(input_dir, "comment_results.csv"), 
                                 os.path.join(output_dir, "comment_results_aggregated.csv"))
