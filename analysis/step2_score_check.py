import pandas as pd
import os
import matplotlib.pyplot as plt
import seaborn as sns

def load_data(directory, file_pattern):
    files = [f for f in os.listdir(directory) if f.endswith('.csv') and file_pattern in f]
    df_list = [pd.read_csv(os.path.join(directory, f)) for f in files if os.path.getsize(os.path.join(directory, f)) > 0]
    return pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()

def calculate_score(row, weights):
    try:
        if 'text_length' not in row or pd.isna(row['text_length']) or row['text_length'] == 0:
            return None

        normalized_features = {k: row[k]/row['text_length'] for k in weights.keys() if 'ratio' in k}
        
        final_score = sum(weights[k] * normalized_features.get(k, 0) for k in normalized_features)
        return final_score
    except Exception as e:
        print(f"Error calculating score for row: {e}")
        return None

def apply_scoring(data, weights):
    if data.empty:
        print("Data is empty. Skipping scoring.")
        return pd.DataFrame()
    return data.assign(final_score=data.apply(lambda row: calculate_score(row, weights), axis=1))

def plot_final_score_matrix(data, user_grps, subreddit_grps, output_dir):
    if data.empty or 'final_score' not in data.columns:
        print("No final score data available. Skipping matrix plot.")
        return

    matrix_data = pd.pivot_table(data, values='final_score', index='user_grp', columns='subreddit_grp', aggfunc='mean')
    matrix_data['ALL'] = matrix_data.mean(axis=1)
    matrix_data = matrix_data.reindex(index=user_grps, columns=subreddit_grps + ['ALL'])

    plt.figure(figsize=(10, 6))
    sns.heatmap(matrix_data, annot=True, cmap='viridis')
    plt.title('Final Score Matrix')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'final_score_matrix.png'))
    print(f"Saved final score matrix plot to {output_dir}")

# Weights for scoring
weights = {
    'noun_ratio': -1.5,
    'verb_ratio': 0,
    'adverb_ratio': 0,
    'first_person_singular_ratio': 1,
    'first_person_plural_ratio': 0,
    'second_person_ratio': 0,
    'third_person_ratio': 0,
    'readability_score': 0,
    'keyword_score': 2,
    'text_length': 0.005
}

# Directory settings
input_dir = './step2/stats/analysis/'
output_dir = './step2/'

# Load data
submissions_data = load_data(input_dir, 'stat_submissions_')
comments_data = load_data(input_dir, 'stat_comments_')

# Apply scoring
submissions_scored = apply_scoring(submissions_data, weights)
comments_scored = apply_scoring(comments_data, weights)

# Save and plot final score matrices
plot_final_score_matrix(submissions_scored, ['SW', 'MH', 'Otr'], ['SW', 'MH', 'Otr'], output_dir)
plot_final_score_matrix(comments_scored, ['SW', 'MH', 'Otr'], ['SW', 'MH', 'Otr'], output_dir)

print("Final score calculations and visualizations completed.")
