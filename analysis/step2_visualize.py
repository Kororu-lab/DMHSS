import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import numpy as np

def load_data(directory, file_pattern):
    """Load data from CSV files in the specified directory matching the given pattern."""
    files = [f for f in os.listdir(directory) if f.endswith('.csv') and file_pattern in f]
    df_list = [pd.read_csv(os.path.join(directory, f)) for f in files if os.path.getsize(os.path.join(directory, f)) > 0]
    return pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()

def plot_bar_grouped(data, features, title, output_dir):
    """Plot grouped bar plots for given features."""
    plt.figure(figsize=(15, 6))
    melted_data = data.melt(id_vars=['user_grp', 'subreddit_grp'], value_vars=features)
    sns.barplot(x='variable', y='value', hue='subreddit_grp', data=melted_data)
    plt.title(title)
    plt.ylabel('Average Value')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, title.replace(" ", "_") + '.png'))

def plot_matrix(data, features, user_grps, subreddit_grps, output_dir):
    """Plot matrix plots for given features."""
    for feature in features:
        numeric_data = data.dropna(subset=[feature]).copy()
        numeric_data[feature] = pd.to_numeric(numeric_data[feature], errors='coerce')
        matrix_data = pd.pivot_table(numeric_data, values=feature, index='user_grp', columns='subreddit_grp', aggfunc='mean')
        matrix_data['ALL'] = matrix_data.mean(axis=1)  # Calculate average for ALL
        matrix_data = matrix_data.reindex(index=user_grps, columns=subreddit_grps + ['ALL'])
        plt.figure(figsize=(10, 6))
        sns.heatmap(matrix_data, annot=True, cmap='viridis')
        plt.title(f'{feature.capitalize()} Matrix')
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, f'{feature}_matrix.png'))

def plot_bar_ratio(data, features, title, output_dir):
    """Plot bar plots using ratio metrics."""
    plt.figure(figsize=(15, 6))
    melted_data = data.melt(id_vars=['user_grp', 'subreddit_grp'], value_vars=features)
    sns.barplot(x='variable', y='value', hue='user_grp', data=melted_data, palette="viridis")
    plt.title(title)
    plt.ylabel('Average Ratio Value')
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, title.replace(" ", "_") + '_ratio.png'))

# Directory settings
input_dir = './step2/stats/'
output_dir = './step2/visualizations/'

# Load data
submissions_data = load_data(input_dir, 'stat_submissions_')
comments_data = load_data(input_dir, 'stat_comments_')

# Define feature groups with ratios
pos_ratio_features = ['noun_ratio', 'verb_ratio', 'adverb_ratio']
pronoun_ratio_features = ['first_person_singular_ratio', 'first_person_plural_ratio', 'second_person_ratio', 'third_person_ratio']
other_features = ['readability_score', 'keyword_score', 'text_length']

# Define user and subreddit groups
user_grps = ['SW', 'MH', 'Otr']
subreddit_grps = ['SW', 'MH', 'Otr']

# Plotting
plot_matrix(submissions_data, other_features, user_grps, subreddit_grps, output_dir)
plot_matrix(comments_data, other_features, user_grps, subreddit_grps, output_dir)
plot_matrix(submissions_data, pos_ratio_features, user_grps, subreddit_grps, output_dir)
plot_matrix(comments_data, pos_ratio_features, user_grps, subreddit_grps, output_dir)
plot_matrix(submissions_data, pronoun_ratio_features, user_grps, subreddit_grps, output_dir)
plot_matrix(comments_data, pronoun_ratio_features, user_grps, subreddit_grps, output_dir)
plot_bar_ratio(submissions_data, pos_ratio_features + pronoun_ratio_features, 'Ratio Metrics in Submissions', output_dir)
plot_bar_ratio(comments_data, pos_ratio_features + pronoun_ratio_features, 'Ratio Metrics in Comments', output_dir)

print("Visualizations saved in:", output_dir)
